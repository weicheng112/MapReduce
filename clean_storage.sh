#!/bin/bash

# Simple script to remove all storage node data

echo "Removing all storage node data..."

# Remove all files in data directories
rm -rf storage/data1/*
rm -rf storage/data2/*
rm -rf storage/data3/*

echo "Storage data removed successfully."