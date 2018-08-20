package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run func(cmd *Command, args []string) error

	// Command name
	Name string

	// Help text
	Help string

	// Flag is a set of flags specific to this command.
	Flag flag.FlagSet
}

var commands = []*Command{
	cmdMeta,
	cmdSchema,
	cmdPages,
	cmdDump,
	cmdCSV,
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: parqueteur [options] command [command options] [parquetfile]\n\n")
	flag.PrintDefaults()

	fmt.Fprintf(os.Stderr, "\n\nSupported commands:\n")
	for _, cmd := range commands {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, " %s - %s\n", cmd.Name, cmd.Help)
		fmt.Fprintf(os.Stderr, " Options:\n")
		cmd.Flag.PrintDefaults()
	}

	os.Exit(2)
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	found := false
	for _, cmd := range commands {
		if cmd.Name == args[0] {
			err := cmd.Flag.Parse(args[1:])
			if err != nil {
				usage()
			}

			found = true
			err = cmd.Run(cmd, cmd.Flag.Args())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
				os.Exit(1)
			}
			break
		}
	}
	if !found {
		usage()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err = pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		_ = f.Close()
	}
}
