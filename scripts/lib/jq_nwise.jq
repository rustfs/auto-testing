# Split an array into fixed-size arrays, including a shorter final batch.
def nwise($size):
  . as $items
  | [range(0; ($items | length); $size) as $offset
     | $items[$offset:$offset + $size]];
