# mqtt-fed

A federator for MQTT brokers. Check out the [publication](https://thescipub.com/abstract/jcssp.2022.687.694).

## Installation

You will need the Rust compiler (version 1.58 or later) and the Rust package manager (Cargo).
Both can be installed using [Rustup](https://www.rust-lang.org/tools/install).

To install mqtt-fed run: 
```
cargo install --git https://github.com/nicolaskribas/mqtt-fed.git
```

## Running

Use the `-c` flag to indicate the config file to the federator, running like this:
```
mqtt-fed -c mqtt-fed.toml
```
Configuration files must be in the [toml format](https://toml.io), an example of configuration file
can be found [here](mqtt-fed.toml).

## [Evaluation scenario](https://github.com/nicolaskribas/federation-scenario)
