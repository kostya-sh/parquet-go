package main

import (
	"flag"
	"fmt"
	"os"
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
	cmdDump,
	cmdPages,
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: parqueteur command [options] [parquetfile]\n\n")

	fmt.Fprintf(os.Stderr, "Supported commands:\n")
	for _, cmd := range commands {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, " %s - %s\n", cmd.Name, cmd.Help)
		fmt.Fprintf(os.Stderr, " Options:\n")
		cmd.Flag.PrintDefaults()
	}

	os.Exit(2)
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		usage()
	}

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			err := cmd.Flag.Parse(args[1:])
			if err != nil {
				usage()
			}

			err = cmd.Run(cmd, cmd.Flag.Args())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	// command not found
	usage()
}
