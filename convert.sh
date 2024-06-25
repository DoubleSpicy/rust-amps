#!/bin/bash

binary_data=$(echo -n -e "\xc0\x92\xf2\x07")

echo -n -e "$binary_data" > output_file
