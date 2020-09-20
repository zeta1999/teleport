package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
)

var plugins = map[string]string{
	"cron": "https://api.github.com/repos/Teleport-Data/teleport-cron/tarball/master",
	"aws-ecs": "https://api.github.com/repos/Teleport-Data/teleport-aws-ecs/tarball/master",
}

const (
	postInstallColor = "\033[1;36m%s\033[0m"
	pluginDirectory  = ".plugins"
)

// InstallPlugin downloads and extracts a plugin to the current Pad directory
func InstallPlugin(name string) error {
	url, ok := plugins[name]
	if !ok {
		return fmt.Errorf("no plugin named '%s'", name)
	}

	file, err := downloadPlugin(url)
	defer os.Remove(file)
	if err != nil {
		return err
	}

	err = extractToPlugins(name, file)
	if err != nil {
		return err
	}

	fmt.Printf("`%s` plugin installed ✓\n", name)

	return printPostInstall(name)
}

// PluginInstalled indicates whether or not a plugin is installed in the current Pad directory
func PluginInstalled(name string) bool {
	_, err := os.Stat(pluginDir(name))

	return !os.IsNotExist(err)
}

// CallPlugin executes the binary within a plugin
func CallPlugin(name string, args []string) error {
	if !PluginInstalled(name) {
		return fmt.Errorf("plugin not installed `%s`", name)
	}

	cmd := exec.Command(filepath.Join(pluginDir(name), "bin", name), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func downloadPlugin(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	tmpFile, err := ioutil.TempFile(os.TempDir(), "*.tar")
	if err != nil {
		return "", err
	}

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", err
	}

	return tmpFile.Name(), nil
}

func extractToPlugins(name string, file string) error {
	err := os.MkdirAll(pluginDir(name), 0755)
	if err != nil {
		return err
	}

	cmd := exec.Command("tar", "xzf", file, "-C", pluginDir(name), "--strip-components", "1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func printPostInstall(name string) error {
	postInstallFile := filepath.Join(pluginDir(name), "postinstall.txt")

	_, err := os.Stat(postInstallFile)
	if err != nil {
		return nil // No postinstall.txt file found, so nothing to print
	}

	bytes, err := ioutil.ReadFile(postInstallFile)
	if err != nil {
		return err
	}

	fmt.Printf(postInstallColor, string(bytes))

	return nil
}

func pluginDir(name string) string {
	return filepath.Join(workingDir(), pluginDirectory, name)
}
