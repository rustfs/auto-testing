#!/usr/bin/env python3
"""Opt-in same-token status transport; no redirects, proxy or request retries."""
from contextlib import contextmanager
import http.client
import threading
from urllib.parse import parse_qsl, urlsplit, quote
import uuid

try:
    from .heal_probe import CLOCK_DOMAIN, Probe, observation_phase, require
except ImportError:
    from heal_probe import CLOCK_DOMAIN, Probe, observation_phase, require


MAX_STATUS_OBSERVATIONS = 1025  # One warmup plus the existing 1024-sample race budget.


class KeepAliveStatusProbe(Probe):
    @contextmanager
    def status_session(self, token, path):
        require(isinstance(token, str) and str(uuid.UUID(token)) == token,
                'canonical status token required')
        require(path == '/rustfs/admin/v3/heal/' + quote(self.binding['bucketPrefix'], safe=''),
                'status session requires the original owned bucket path')
        require(getattr(self, '_status', None) is None, 'nested status session prohibited')
        endpoint = urlsplit(self.endpoint)
        connection_type = http.client.HTTPSConnection if endpoint.scheme == 'https' else http.client.HTTPConnection
        state = dict(token=token, path=path, owner=threading.get_ident(), poisoned=False, requests=0,
                     transport='direct-http-keepalive', implicitRetriesAllowed=False,
                     foregroundDelayInjected=False, observationSchemaVersion=1,
                     sessionId=str(uuid.uuid4()), clockDomain=CLOCK_DOMAIN,
                     observationBudget=MAX_STATUS_OBSERVATIONS, observations=[],
                     connectionAttempts=0, connectionsOpened=0)
        probe = self

        class ObservedConnection(connection_type):
            def connect(connection_self):
                state['connectionAttempts'] += 1
                with observation_phase(probe._status_observation, 'connectionEstablishment'):
                    super().connect()
                state['connectionsOpened'] += 1
                connection_self.observed_socket = connection_self.sock
                connection_self.observed_id = f"{state['sessionId']}:{state['connectionsOpened']}"

        connection = ObservedConnection(endpoint.hostname, endpoint.port, timeout=30)
        self._status_observation = None
        self._status = (connection, state)
        try:
            yield state
        finally:
            connection.close()
            self._status = None
            self._status_observation = None

    @contextmanager
    def status_observation(self):
        session = getattr(self, '_status', None)
        if session is None:
            yield None
            return
        connection, state = session
        observation = None
        owns_observation = False
        try:
            require(threading.get_ident() == state['owner'], 'status session thread changed')
            require(not state['poisoned'], 'status session poisoned; no implicit retry')
            observation = self._status_observation
            if observation is None:
                require(len(state['observations']) < state['observationBudget'], 'status observation budget exhausted')
                observation = dict(sessionId=state['sessionId'], sequence=len(state['observations']) + 1,
                                   clockDomain=CLOCK_DOMAIN, phases={}, requestWriteCompleted=False,
                                   responseObserved=False, responsePersisted=False)
                state['observations'].append(observation)
                self._status_observation = observation
                owns_observation = True
            yield observation
        except BaseException as exc:
            if observation is not None:
                observation.setdefault('failureType', type(exc).__name__)
            state['poisoned'] = True
            state.setdefault('failureType', type(exc).__name__)
            connection.close()
            raise
        finally:
            if owns_observation:
                self._status_observation = None

    def observation_for(self, sample):
        session = getattr(self, '_status', None)
        require(session is not None, 'status observation session required')
        _, state = session
        require(threading.get_ident() == state['owner'] and not state['poisoned'], 'status observer unavailable')
        require(sample.get('clockDomain') == state['clockDomain'], 'status observation clock changed')
        require(sample.get('query') == [('clientToken', state['token'])], 'status observation token changed')
        reference = sample.get('statusObservation') or {}
        require(type(reference.get('sequence')) is int
                and reference == dict(sessionId=state['sessionId'], sequence=len(state['observations']))
                and bool(state['observations']), 'status observation reference changed')
        return state['observations'][-1]

    def _open_admin(self, outgoing):
        session = getattr(self, '_status', None)
        if session is None:
            return super()._open_admin(outgoing)
        connection, state = session
        target = urlsplit(outgoing.full_url)
        endpoint = urlsplit(self.endpoint)
        require(threading.get_ident() == state['owner'], 'status session thread changed')
        require(not state['poisoned'], 'status session poisoned; no implicit retry')
        require((target.scheme, target.netloc) == (endpoint.scheme, endpoint.netloc)
                and target.path == state['path'] and not target.fragment
                and parse_qsl(target.query, keep_blank_values=True) == [('clientToken', state['token'])]
                and outgoing.get_method() == 'POST' and outgoing.data == b'',
                'keepalive permits only the original token status; no control mutation')
        state['requests'] += 1
        observation = self._status_observation
        try:
            opened_before = state['connectionsOpened']
            with observation_phase(observation, 'requestWrite'):
                connection.request('POST', target.path + '?' + target.query, body=b'',
                                   headers=dict(outgoing.header_items()))
            if observation is not None:
                observation['requestWriteCompleted'] = True
                sock = connection.sock
                require(sock is not None and sock is connection.observed_socket,
                        'status socket identity unavailable after request write')
                observation.update(transport=dict(
                    requestSequence=state['requests'], connectionId=connection.observed_id,
                    newConnection=state['connectionsOpened'] > opened_before,
                    socketDescriptor=sock.fileno(), localAddress=list(sock.getsockname()),
                    peerAddress=list(sock.getpeername())))
            with observation_phase(observation, 'responseHeaders'):
                response = connection.getresponse()
            if observation is not None:
                observation['transport']['serverWillClose'] = response.will_close
            # Probe owns the bounded full-body read and records the same timing,
            # headers and response as its default urllib path. Closing a fully
            # consumed HTTPResponse retains a server-supported keepalive socket.
            response.code = response.status
            if 300 <= response.status < 400:
                response.close()
                raise ValueError('Admin redirects are prohibited')
            return response
        except BaseException:
            state['poisoned'] = True
            connection.close()
            raise

    def admin(self, *args, **kwargs):
        with self.status_observation() as observation:
            provided = kwargs.pop('observation', None)
            require(provided is None or provided is observation, 'status observation owner changed')
            require(observation is None or 'callStartedMonotonicNs' not in observation,
                    'status observation already invoked')
            return super().admin(*args, **kwargs, observation=observation)
