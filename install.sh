#!/usr/bin/env bash
brew install dep
brew upgrade dep
dep init
dep ensure -v
