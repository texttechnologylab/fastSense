#!/bin/bash
sudo docker run -p 5000:80 -it --rm --name fastsense-en -v $(pwd)/best_model:/model textimager-fastsense-en
