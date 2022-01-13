#!/bin/bash
for i in dbd-920dbcs41257/*.csv
do
    bn=$(basename "$i" .csv)
    ./analyze.py "$bn" | tee "analysis/$bn.csv"
done
