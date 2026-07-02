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
	"strings"

	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

func HandleRestore(args []string) {
	restoreCmd := flag.NewFlagSet("restore", flag.ExitOnError)
	fileFlag := restoreCmd.String("file", "", "Backup file to restore (.zip or .tar.gz)")
	dataDirFlag := restoreCmd.String("data-dir", "./data", "Target data directory (default: ./data)")
	restoreCmd.Parse(args)
	if *fileFlag == "" {
		fmt.Printf("%s[Error] --file is required. Example: tmq restore --file tinymq_backup_1234.tar.gz%s\n", shared.ColorRed, shared.ColorReset)
		return
	}
	filename := *fileFlag
	dataDir := *dataDirFlag
	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
				fmt.Printf("%s[Warn] '%s' already contains .log files. Restoring will overwrite them.%s\n", shared.ColorYellow, dataDir, shared.ColorReset)
				fmt.Printf("Continue? [y/N]: ")
				var answer string
				fmt.Scanln(&answer)
				if strings.ToLower(strings.TrimSpace(answer)) != "y" {
					fmt.Println("Restore cancelled.")
					return
				}
				break
			}
		}
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("%s[Error] Cannot create data directory '%s': %v%s\n", shared.ColorRed, dataDir, err, shared.ColorReset)
		return
	}
	var err error
	switch {
	case strings.HasSuffix(filename, ".tar.gz") || strings.HasSuffix(filename, ".tgz"):
		err = restoreFromTarGz(filename, dataDir)
	case strings.HasSuffix(filename, ".zip"):
		err = restoreFromZip(filename, dataDir)
	default:
		fmt.Printf("%s[Error] Unrecognised file format. Expected .zip or .tar.gz%s\n", shared.ColorRed, shared.ColorReset)
		return
	}
	if err != nil {
		fmt.Printf("%s[Error] Restore failed: %v%s\n", shared.ColorRed, err, shared.ColorReset)
		return
	}
	fmt.Printf("%s✔ Restore complete! Data written to '%s'%s\n", shared.ColorGreen, dataDir, shared.ColorReset)
	fmt.Printf("%s  Restart TinyMQ to load the recovered messages.%s\n", shared.ColorYellow, shared.ColorReset)
}
func restoreFromTarGz(filename, dataDir string) error {
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("cannot read gzip stream: %w", err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	restored := 0
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg || !strings.HasSuffix(header.Name, ".log") {
			continue
		}
		entryName := filepath.Base(header.Name)
		if entryName == "." || entryName == ".." {
			continue
		}
		destPath := filepath.Join(dataDir, entryName)
		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("cannot create '%s': %w", destPath, err)
		}
		if _, err := io.Copy(out, io.LimitReader(tr, 512<<20)); err != nil {
			out.Close()
			return fmt.Errorf("error writing '%s': %w", entryName, err)
		}
		out.Close()
		restored++
		fmt.Printf("  ↳ restored: %s\n", entryName)
	}
	if restored == 0 {
		return fmt.Errorf("archive contained no .log files")
	}
	fmt.Printf("  %d topic log(s) restored.\n", restored)
	return nil
}
func restoreFromZip(filename, dataDir string) error {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return fmt.Errorf("cannot open zip file: %w", err)
	}
	defer r.Close()
	restored := 0
	for _, f := range r.File {
		if f.FileInfo().IsDir() || !strings.HasSuffix(f.Name, ".log") {
			continue
		}
		entryName := filepath.Base(f.Name)
		if entryName == "." || entryName == ".." {
			continue
		}
		destPath := filepath.Join(dataDir, entryName)
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("cannot open zip entry '%s': %w", f.Name, err)
		}
		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			rc.Close()
			return fmt.Errorf("cannot create '%s': %w", destPath, err)
		}
		if _, err := io.Copy(out, io.LimitReader(rc, 512<<20)); err != nil {
			rc.Close()
			out.Close()
			return fmt.Errorf("error writing '%s': %w", entryName, err)
		}
		rc.Close()
		out.Close()
		restored++
		fmt.Printf("  ↳ restored: %s\n", entryName)
	}
	if restored == 0 {
		return fmt.Errorf("archive contained no .log files")
	}
	fmt.Printf("  %d topic log(s) restored.\n", restored)
	return nil
}
