# Consistent HashRing

Consistent Hashring with max length 2^32, safe for concurrent operation

## Getting Started



### Usage



```
import chr "github.com/cgyim1992/consistentHashRing"
import "hash/crc32"

node_list := []uint32{10000, 40000, 16000000, 89999999}
ring := chr.NewConsistentHashRing(crc32.ChecksumIEEE, 4, node_list)
ring.add([]byte{0x1,0x2,0x3,0x4})
ring.get(12345678)
ring.delete(87654321)



```
you can use any hash function that hashes byte slice to unsigned int32. The example uses crc32 as template.
node list refers to the real physical node list, you can make a conversion from ipv4 or ipv6 to uint32 by yourself.

### Installing


```
go get github.com/cgyim1992/consistentHashRing
```





