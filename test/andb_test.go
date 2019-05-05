package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	syncpkg "sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ANDB", func() {
	var storeDir string

	BeforeEach(func() {
		var err error
		storeDir, err = ioutil.TempDir("", "andb_test")
		Expect(err).NotTo(HaveOccurred())

		startServer(storeDir)
	})

	AfterEach(func() {
		stopServer()
		Expect(os.RemoveAll(storeDir)).To(Succeed())
	})

	It("stores and deletes stuff", func() {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			set(key, value)
		}

		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}

		for i := 3; i < 7; i++ {
			key := fmt.Sprintf("key-%d", i)
			delete(key)
		}

		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}

		for i := 3; i < 7; i++ {
			key := fmt.Sprintf("key-%d", i)
			output, err := getWithError(key)
			Expect(err).To(HaveOccurred())
			Expect(output).To(Equal("error: not found"))
		}

		for i := 7; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}
	})

	It("stores stuff across reboots", func() {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			set(key, value)
		}

		for i := 3; i < 7; i++ {
			key := fmt.Sprintf("key-%d", i)
			delete(key)
		}

		rebootServer(storeDir)

		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}

		for i := 3; i < 7; i++ {
			key := fmt.Sprintf("key-%d", i)
			output, err := getWithError(key)
			Expect(err).To(HaveOccurred())
			Expect(output).To(Equal("error: not found"))
		}

		for i := 7; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}
	})

	XContext("when a write fails", func() {
		BeforeEach(func() {
			// TODO: this doesn't work! The go stdlib keeps writing stuff!
			Expect(os.RemoveAll(filepath.Join(storeDir, "andbdata.bin"))).To(Succeed())

			_, err := setWithError("key", "value")
			Expect(err).To(HaveOccurred())
		})

		It("returns 'not found' from get", func() {
			output, err := getWithError("key")
			Expect(err).To(HaveOccurred())
			Expect(output).To(Equal("error: not found"))

			rebootServer(storeDir)

			output, err = getWithError("key")
			Expect(err).To(HaveOccurred())
			Expect(output).To(Equal("error: not found"))
		})
	})

	Context("when the filestore is corrupted", func() {
		var (
			metaBytes, dataBytes []byte
		)

		BeforeEach(func() {
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				set(key, value)
			}

			var err error
			metaBytes, err = ioutil.ReadFile(filepath.Join(storeDir, "andbmeta.bin"))
			Expect(err).NotTo(HaveOccurred())
			dataBytes, err = ioutil.ReadFile(filepath.Join(storeDir, "andbdata.bin"))
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(ioutil.WriteFile(filepath.Join(storeDir, "andbmeta.bin"), metaBytes, 0600)).To(Succeed())
			Expect(ioutil.WriteFile(filepath.Join(storeDir, "andbdata.bin"), dataBytes, 0600)).To(Succeed())

			rebootServer(storeDir)

			output, err := getWithError("key-0")
			Expect(err).To(HaveOccurred())
			Expect(output).To(ContainSubstring("error: load store: for each block: block handler: incorrect"))
		})

		It("gracefully handles a block version being wrong", func() {
			metaBytes[0] = 0xFF
		})

		It("gracefully handles a key offset being wrong", func() {
			metaBytes[4] = 0xFF
		})

		It("gracefully handles a key length being wrong", func() {
			metaBytes[8] = 0xFF
		})

		It("gracefully handles a key crc32 being wrong", func() {
			metaBytes[12] = 0xFF
		})

		It("gracefully handles a key being wrong", func() {
			dataBytes[0] = 0xFF
		})

		It("gracefully handles a value offset being wrong", func() {
			metaBytes[16] = 0xFF
		})

		It("gracefully handles a value length being wrong", func() {
			metaBytes[20] = 0xFF
		})

		It("gracefully handles a value crc32 being wrong", func() {
			metaBytes[24] = 0xFF
		})

		It("gracefully handles a value being wrong", func() {
			dataBytes[5] = 0xFF
		})
	})

	It("handles concurrency gracefully", func() {
		wg := syncpkg.WaitGroup{}
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer GinkgoRecover()
				defer wg.Done()

				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				set(key, value)
			}(i)
		}
		wg.Wait()

		sync()
		rebootServer(storeDir)

		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer GinkgoRecover()
				defer wg.Done()

				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				Expect(get(key)).To(Equal(value))
			}(i)
		}
		wg.Wait()
	})

	XIt("can run multiple services on top of one backing store", func() {
	})

	XIt("can run in async write mode or sync write mode", func() {
	})

	XIt("defragments the data storage file over time", func() {
	})

	XContext("performance", func() {
		Measure("appending writes", func(b Benchmarker) {
			writing := b.Time("writing", func() {
				for i := 0; i < 1000; i++ {
					key := fmt.Sprintf("key-%d", i)
					value := fmt.Sprintf("value-%d", i)
					set(key, value)

					if i%100 == 0 {
						fmt.Printf("set %d values\n", i)
					}
				}
			})
			Expect(writing).To(BeNumerically("<", time.Duration(1*time.Second)))
		}, 5)

		Measure("random, non-overlapping writes", func(b Benchmarker) {
			writing := b.Time("writing", func() {
				for i := 0; i < 1000; i++ {
					key := fmt.Sprintf("key-%d", i)
					value := fmt.Sprintf("value-%d", i)
					set(key, value)

					if i%100 == 0 {
						fmt.Printf("set %d values\n", i)
					}
				}
			})
			Expect(writing).To(BeNumerically("<", time.Duration(1*time.Second)))
		}, 5)

		Measure("random, overlapping writes", func(b Benchmarker) {
			writing := b.Time("writing", func() {
				for i := 0; i < 1000; i++ {
					key := fmt.Sprintf("key-%d", i)
					value := fmt.Sprintf("value-%d", i)
					set(key, value)

					if i%100 == 0 {
						fmt.Printf("set %d values\n", i)
					}
				}
			})
			Expect(writing).To(BeNumerically("<", time.Duration(1*time.Second)))
		}, 5)

		Measure("sequential deletes", func(b Benchmarker) {
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				set(key, value)

				if i%100 == 0 {
					fmt.Printf("set %d values\n", i)
				}
			}

			deleting := b.Time("deleting", func() {
				for i := 0; i < 1000; i++ {
					key := fmt.Sprintf("key-%d", i)
					delete(key)
				}
			})
			Expect(deleting).To(BeNumerically("<", time.Duration(1*time.Second)))
		}, 5)
	})
})
