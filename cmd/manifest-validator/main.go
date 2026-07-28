package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(paths []string) error {
	if len(paths) == 0 {
		return errors.New("usage: manifest-validator <file-or-directory>...")
	}
	var files []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		if !info.IsDir() {
			files = append(files, path)
			continue
		}
		err = filepath.WalkDir(path, func(candidate string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() && (strings.HasSuffix(candidate, ".yaml") || strings.HasSuffix(candidate, ".yml")) {
				files = append(files, candidate)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk %q: %w", path, err)
		}
	}
	for _, path := range files {
		if err := validate(path); err != nil {
			return err
		}
	}
	fmt.Printf("validated %d YAML files\n", len(files))
	return nil
}

func validate(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	for document := 1; ; document++ {
		var value any
		err := decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode %q document %d: %w", path, document, err)
		}
	}
}
