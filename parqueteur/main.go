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
}

func usage() {
	fmt.Println("Usage: parqueteur command [options] [parquetfile]")
	fmt.Println("")

	fmt.Println("Supported commands:")
	for _, cmd := range commands {
		fmt.Println("")
		fmt.Printf(" %s - %s\n", cmd.Name, cmd.Help)
		fmt.Printf(" Options:\n")
		cmd.Flag.PrintDefaults()
	}

	os.Exit(2)
}

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			cmd.Flag.Parse(args[1:])
			err := cmd.Run(cmd, cmd.Flag.Args())
			if err != nil {
				fmt.Printf("Error: %s\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	// command not found
	usage()
}
