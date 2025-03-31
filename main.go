package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"unsafe"

	"github.com/dgraph-io/ristretto"
)

const (
	CommandPut byte = 0x01
	CommandGet byte = 0x02

	StatusSuccess byte = 0x00
	StatusError   byte = 0x01

	MaxKeyLength   = 256
	MaxValueLength = 256
)

// Error message constants
const (
	ErrKeyTooLong    = "key exceeds maximum length"
	ErrValueTooLong  = "value exceeds maximum length"
	ErrKeyNotFound   = "key not found"
	ErrUnknownCmd    = "unknown command"
	ErrInvalidFormat = "invalid request format"
)

// Pre-built error responses to avoid repeated allocations
var (
	ErrorResponses = map[string][]byte{
		ErrKeyTooLong:    buildErrorResponse([]byte(ErrKeyTooLong)),
		ErrValueTooLong:  buildErrorResponse([]byte(ErrValueTooLong)),
		ErrKeyNotFound:   buildErrorResponse([]byte(ErrKeyNotFound)),
		ErrUnknownCmd:    buildErrorResponse([]byte(ErrUnknownCmd)),
		ErrInvalidFormat: buildErrorResponse([]byte(ErrInvalidFormat)),
	}
)

// Helper function to build error responses
func buildErrorResponse(msg []byte) []byte {
	resp := make([]byte, 1+2+len(msg))
	resp[0] = StatusError
	binary.BigEndian.PutUint16(resp[1:3], uint16(len(msg)))
	copy(resp[3:], msg)
	return resp
}

// unsafeBytesToString converts a byte slice to string without copying.
// The caller must ensure the byte slice is not modified after conversion.
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ShardedCache represents a cache divided into multiple shards for concurrency.
type ShardedCache struct {
	shards     []*ristretto.Cache
	shardCount int
}

// NewShardedCache creates a new sharded cache with the given number of shards and capacity per shard.
func NewShardedCache(shardCount int, capacity int64) *ShardedCache {
	shards := make([]*ristretto.Cache, shardCount)
	for i := 0; i < shardCount; i++ {
		cache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: 10 * capacity, // More counters for better hit ratios
			MaxCost:     capacity,       // MaxCost corresponds to the max number of items per shard
			BufferItems: 64,             // Adjusted for higher throughput
		})
		if err != nil {
			log.Fatalf("Failed to create cache shard: %v", err)
		}
		shards[i] = cache
	}
	return &ShardedCache{
		shards:     shards,
		shardCount: shardCount,
	}
}

// getShard determines which shard a key belongs to using FNV hashing.
func (sc *ShardedCache) getShard(key []byte) *ristretto.Cache {
	h := fnvHash(key)
	return sc.shards[h%uint32(sc.shardCount)]
}

// fnvHash calculates a hash value for a given key using FNV hashing.
func fnvHash(key []byte) uint32 {
	var hash uint32 = 2166136261
	for _, b := range key {
		hash *= 16777619
		hash ^= uint32(b)
	}
	return hash
}

// Put inserts or updates a key-value pair in the appropriate shard.
func (sc *ShardedCache) Put(key, value []byte) error {
	if len(key) > MaxKeyLength {
		return fmt.Errorf("key too long")
	}
	if len(value) > MaxValueLength {
		return fmt.Errorf("value too long")
	}
	shard := sc.getShard(key)
	keyStr := unsafeBytesToString(key)
	valueStr := unsafeBytesToString(value)
	if !shard.Set(keyStr, valueStr, 1) {
		return fmt.Errorf("failed to insert key into cache")
	}
	return nil
}

// Get retrieves a value from the appropriate shard.
func (sc *ShardedCache) Get(key []byte) ([]byte, error) {
	shard := sc.getShard(key)
	keyStr := unsafeBytesToString(key)
	value, found := shard.Get(keyStr)
	if !found {
		return nil, fmt.Errorf("key not found")
	}
	// Convert the returned string to []byte
	return []byte(value.(string)), nil
}

// Server encapsulates the cache and provides TCP handlers.
type Server struct {
	cache        *ShardedCache
	responsePool *sync.Pool
}

// NewServer initializes a new Server instance.
func NewServer(cache *ShardedCache) *Server {
	return &Server{
		cache: cache,
		responsePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate a buffer for GET success responses
				return make([]byte, 1+2+MaxValueLength)
			},
		},
	}
}

// handleConnection manages a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	readBuf := make([]byte, 4096) // Buffer for reading data
	buffer := make([]byte, 0, 4096)

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			// Connection closed or read error
			return
		}
		if n == 0 {
			continue
		}

		// Append newly read data to the buffer
		buffer = append(buffer, readBuf[:n]...)

		// Process as many complete requests as possible
		processOffset := 0
		bufferLen := len(buffer)

		for processOffset < bufferLen {
			// At least 1 byte for command
			if bufferLen-processOffset < 1 {
				break
			}

			cmd := buffer[processOffset]
			processOffset++

			switch cmd {
			case CommandPut:
				// Need 2 bytes for key length
				if bufferLen-processOffset < 2 {
					processOffset-- // Rewind to the command byte
					break
				}
				keyLen := binary.BigEndian.Uint16(buffer[processOffset : processOffset+2])
				processOffset += 2

				if keyLen > MaxKeyLength {
					// Send key too long error
					if resp, ok := ErrorResponses["key too long"]; ok {
						conn.Write(resp)
					} else {
						resp := buildErrorResponse([]byte("key exceeds maximum length"))
						conn.Write(resp)
					}

					// Attempt to skip key and value length
					if bufferLen-processOffset >= int(keyLen)+2 {
						processOffset += int(keyLen)
						valueLen := binary.BigEndian.Uint16(buffer[processOffset : processOffset+2])
						processOffset += 2 + int(valueLen)
					} else {
						// Not enough data, rewind
						processOffset -= 3
						break
					}
					continue
				}

				// Ensure enough data for key and value length
				if bufferLen-processOffset < int(keyLen)+2 {
					processOffset -= 3 // Rewind to command byte
					break
				}

				key := buffer[processOffset : processOffset+int(keyLen)]
				processOffset += int(keyLen)

				// Read value length
				valueLen := binary.BigEndian.Uint16(buffer[processOffset : processOffset+2])
				processOffset += 2

				if valueLen > MaxValueLength {
					// Send value too long error
					if resp, ok := ErrorResponses["value too long"]; ok {
						conn.Write(resp)
					} else {
						resp := buildErrorResponse([]byte("value exceeds maximum length"))
						conn.Write(resp)
					}

					// Attempt to skip value
					if bufferLen-processOffset >= int(valueLen) {
						processOffset += int(valueLen)
					} else {
						// Not enough data, rewind
						processOffset -= (1 + 2 + int(keyLen) + 2)
						break
					}
					continue
				}

				// Ensure enough data for value
				if bufferLen-processOffset < int(valueLen) {
					// Not enough data, rewind
					processOffset -= (1 + 2 + int(keyLen) + 2)
					break
				}

				value := buffer[processOffset : processOffset+int(valueLen)]
				processOffset += int(valueLen)

				// Perform PUT operation
				err := s.cache.Put(key, value)
				if err != nil {
					// Send error response
					msg := []byte(err.Error())
					resp := make([]byte, 1+2+len(msg))
					resp[0] = StatusError
					binary.BigEndian.PutUint16(resp[1:3], uint16(len(msg)))
					copy(resp[3:], msg)
					conn.Write(resp)
				} else {
					// Send PUT success response
					conn.Write([]byte{StatusSuccess})
				}

			case CommandGet:
				// Need 2 bytes for key length
				if bufferLen-processOffset < 2 {
					processOffset-- // Rewind to command byte
					break
				}
				keyLen := binary.BigEndian.Uint16(buffer[processOffset : processOffset+2])
				processOffset += 2

				if keyLen > MaxKeyLength {
					// Send key too long error
					if resp, ok := ErrorResponses["key too long"]; ok {
						conn.Write(resp)
					} else {
						resp := buildErrorResponse([]byte("key exceeds maximum length"))
						conn.Write(resp)
					}

					// Attempt to skip key
					if bufferLen-processOffset >= int(keyLen) {
						processOffset += int(keyLen)
					} else {
						// Not enough data, rewind
						processOffset -= 3
						break
					}
					continue
				}

				// Ensure enough data for key
				if bufferLen-processOffset < int(keyLen) {
					processOffset -= 3 // Rewind to command byte
					break
				}

				key := buffer[processOffset : processOffset+int(keyLen)]
				processOffset += int(keyLen)

				// Perform GET operation
				value, err := s.cache.Get(key)
				if err != nil {
					// Send key not found error
					if resp, ok := ErrorResponses["key not found"]; ok {
						conn.Write(resp)
					} else {
						resp := buildErrorResponse([]byte("key not found"))
						conn.Write(resp)
					}
				} else {
					// Prepare GET success response
					valueLen := uint16(len(value))
					resp := s.responsePool.Get().([]byte)
					resp = resp[:1+2+len(value)]
					resp[0] = StatusSuccess
					binary.BigEndian.PutUint16(resp[1:3], valueLen)
					copy(resp[3:], value)
					conn.Write(resp)
					s.responsePool.Put(resp[:0])
				}

			default:
				// Unknown command
				if resp, ok := ErrorResponses["unknown command"]; ok {
					conn.Write(resp)
				} else {
					resp := buildErrorResponse([]byte("unknown command"))
					conn.Write(resp)
				}
			}
		}

		// Remove processed data from buffer
		if processOffset > 0 {
			if processOffset >= len(buffer) {
				buffer = buffer[:0]
			} else {
				copy(buffer, buffer[processOffset:])
				buffer = buffer[:len(buffer)-processOffset]
			}
		}

		// Prevent buffer from growing indefinitely
		if len(buffer) > 4096 {
			buffer = buffer[:0]
		}
	}
}

func main() {
	// Utilize all available CPU cores for maximum concurrency
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize cache with optimal shard count and capacity
	shardCount := 256 // Increased number of shards for better CPU utilization and reduced lock contention
	capacityPerShard := int64(400000)
	cache := NewShardedCache(shardCount, capacityPerShard)
	server := NewServer(cache)

	// Start listening on TCP port 7171
	listener, err := net.Listen("tcp", ":7171")
	if err != nil {
		log.Fatalf("Failed to listen on port 7171: %v", err)
	}
	defer listener.Close()

	log.Println("Key-Value Cache Server listening on port 7171")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		// Handle connection in a new goroutine
		go server.handleConnection(conn)
	}
}