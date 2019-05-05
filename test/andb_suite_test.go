package test

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	andbClient, andbServer, andbStoreReader string

	andbServerSession *gexec.Session
)

func TestAndb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ANDB Suite")
}

var _ = BeforeSuite(func() {
	var err error
	andbClient, err = gexec.Build("github.com/ankeesler/andb/cmd/andb")
	Expect(err).NotTo(HaveOccurred())

	andbServer, err = gexec.Build("github.com/ankeesler/andb/cmd/andbserver")
	Expect(err).NotTo(HaveOccurred())

	andbStoreReader, err = gexec.Build("github.com/ankeesler/andb/cmd/andbstorereader")
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})

func startServer(storeDir string) {
	var err error
	cmd := exec.Command(andbServer, "-storedir", storeDir)
	andbServerSession, err = gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	healthy := false
	for i := 0; i < 3; i++ {
		time.Sleep(time.Millisecond * 50)
		_, err := setWithError("healthcheck", "green")
		if err == nil {
			healthy = true
			break
		} else {
			fmt.Fprintf(GinkgoWriter, "waiting for andbserver to be healthy (error: %s)\n", err.Error())
		}
	}
	Expect(healthy).To(BeTrue(), "andbserver did not come up within 3 healthchecks!")
}

func stopServer() {
	andbServerSession.Kill().Wait(time.Second * 3)
}

func rebootServer(storeDir string) {
	stopServer()
	startServer(storeDir)
}

func getWithError(key string) (string, error) {
	output, err := exec.Command(andbClient, "get", key).CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func get(key string) string {
	output, err := getWithError(key)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), string(output))
	return output
}

func setWithError(key, value string) (string, error) {
	output, err := exec.Command(andbClient, "set", key, value).CombinedOutput()
	return string(output), err
}

func set(key, value string) {
	output, err := setWithError(key, value)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), output)
}

func deleteWithError(key string) (string, error) {
	output, err := exec.Command(andbClient, "delete", key).CombinedOutput()
	return string(output), err
}

func delete(key string) {
	output, err := deleteWithError(key)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), string(output))
}

func sync() {
	output, err := exec.Command(andbClient, "sync").CombinedOutput()
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), string(output))
}

func printStore(storeDir string) {
	output, err := exec.Command(andbStoreReader, storeDir).CombinedOutput()
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), string(output))
	fmt.Println(string(output))
}
