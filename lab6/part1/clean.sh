#!/bin/bash
rm -rf *_classes
rm *.jar
hadoop fs -rm -r -f bigram
hadoop fs -rm -r -f rank
