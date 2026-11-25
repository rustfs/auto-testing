"""Unit tests for the S3 bulk-delete key fixtures."""

import unittest

from scripts.s3_bulk_delete_keys import KEY_SHAPES, make_keys


class BulkDeleteKeysTest(unittest.TestCase):
    def test_short_preserves_original_keys(self):
        self.assertEqual(make_keys(3, "short"), ["target/0000", "target/0001", "target/0002"])

    def test_comparisons_control_length_and_depth(self):
        short, = make_keys(1, "short-shallow")
        shallow, = make_keys(1, "long-shallow")
        deep, = make_keys(1, "long-deep")

        self.assertLess(len(short.encode()), len(shallow.encode()))
        self.assertEqual(len(short.split("/")), len(shallow.split("/")))
        self.assertEqual(len(shallow.split("/")) - 1, 4)
        self.assertEqual(len(deep.split("/")) - 1, 32)
        for key in (shallow, deep):
            self.assertEqual(len(key.encode("utf-8")), 1024)
            self.assertTrue(all(0 < len(part.encode("utf-8")) <= 255 for part in key.split("/")))

    def test_all_shapes_support_a_full_unique_reproducible_batch(self):
        self.assertEqual(set(KEY_SHAPES), {"short", "short-shallow", "long-shallow", "long-deep"})
        for shape in KEY_SHAPES:
            with self.subTest(shape=shape):
                keys = make_keys(1000, shape)
                self.assertEqual(len(keys), 1000)
                self.assertEqual(len(set(keys)), 1000)
                self.assertTrue(all(key.startswith("target/") for key in keys))
                self.assertEqual([key[-4:] for key in keys], [f"{n:04d}" for n in range(1000)])
                self.assertEqual(make_keys(32, shape), keys[:32])
                self.assertEqual(make_keys(0, shape), [])

    def test_unsupported_shapes_and_batch_sizes(self):
        for count in (-1, 1001, 1.5, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                make_keys(count, "short")
        with self.assertRaises(ValueError):
            make_keys(1, "unknown")


if __name__ == "__main__":
    unittest.main()
