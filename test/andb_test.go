package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

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

	It("stores stuff", func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			set(key, value)
		}

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			Expect(get(key)).To(Equal(value))
		}

		// TODO: deletes
	})

	It("stores stuff across reboots", func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			set(key, value)
		}

		// TODO: deletes

		rebootServer(storeDir)

		for i := 0; i < 100; i++ {
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
		wg := sync.WaitGroup{}
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
})
