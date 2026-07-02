package handle

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

func HandleBackup(args []string) {
	backupCmd := flag.NewFlagSet("backup", flag.ExitOnError)
	format := backupCmd.String("format", "zip", "Backup format: 'zip' or 'tar'")
	backupCmd.Parse(args)
	dataDir := "./data"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("%s[Error] Cannot find './data' directory.%s\n", shared.ColorRed, shared.ColorReset)
		return
	}
	if *format == "tar" {
		outName := fmt.Sprintf("tinymq_backup_%d.tar.gz", time.Now().Unix())
		createTarGzBackup(dataDir, outName)
	} else {
		outName := fmt.Sprintf("tinymq_backup_%d.zip", time.Now().Unix())
		createZipBackup(dataDir, outName)
	}
}

func createZipBackup(dataDir string, outZip string) {
	fmt.Printf("Backing up TinyMQ data to '%s' (Format: ZIP)...\n", outZip)
	outFile, err := os.Create(outZip)
	if err != nil {
		fmt.Printf("❌ Error creating zip file: %v\n", err)
		return
	}
	defer outFile.Close()
	w := zip.NewWriter(outFile)
	defer w.Close()
	filesAdded := 0
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}
		relPath, _ := filepath.Rel(dataDir, path)
		f, err := w.Create(relPath)
		if err != nil {
			return err
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		if _, err = io.Copy(f, src); err == nil {
			filesAdded++
		}
		return nil
	})
	fmt.Printf("✅ Backup complete! %d topic logs compressed securely.\n", filesAdded)
}

func createTarGzBackup(dataDir string, outTar string) {
	fmt.Printf("Backing up TinyMQ data to '%s' (Format: TAR.GZ)...\n", outTar)
	outFile, err := os.Create(outTar)
	if err != nil {
		fmt.Printf("❌ Error creating tar file: %v\n", err)
		return
	}
	defer outFile.Close()
	gw := gzip.NewWriter(outFile)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	filesAdded := 0
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		relPath, _ := filepath.Rel(dataDir, path)
		header.Name = relPath
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if _, err = io.Copy(tw, src); err == nil {
			filesAdded++
		}
		return nil
	})
	fmt.Printf("✅ Backup complete! %d topic logs compressed securely.\n", filesAdded)
}
