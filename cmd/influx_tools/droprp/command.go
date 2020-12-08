package droprp

import (
	"errors"
	"flag"
	"fmt"

	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/tsdb"
)

// Command displays help for command-line sub-commands.
type Command struct {
	server server.Interface

	configPath    string
	database      string
	rp            string
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		server: server,
	}
}

// Run executes the command.
func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}
	defer cmd.server.Close()

	client := cmd.server.MetaClient()

	dbi := client.Database(cmd.database)
	if dbi == nil {
		return fmt.Errorf("database '%s' does not exist", cmd.database)
	}

	if dbi.RetentionPolicy(cmd.rp) == nil {
		return fmt.Errorf("rp '%s' does not exist", cmd.rp)
	}

	store := tsdb.NewStore(cmd.server.TSDBConfig().Dir)
	if cmd.server.Logger() != nil {
		store.WithLogger(cmd.server.Logger())
	}

	store.EngineOptions.Config = cmd.server.TSDBConfig()
	store.EngineOptions.EngineVersion = cmd.server.TSDBConfig().Engine
	store.EngineOptions.IndexVersion = cmd.server.TSDBConfig().Index

	err = store.Open()
	if err != nil {
		return err
	}
	defer store.Close()

	// Locally drop the retention policy.
	if err := store.DeleteRetentionPolicy(cmd.database, cmd.rp); err != nil {
		return err
	}

	return client.DropRetentionPolicy(cmd.database, cmd.rp)
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("droprp", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.rp == "" {
		return errors.New("rp is required")
	}

	return nil
}
