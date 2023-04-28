#!/bin/sh
# schemacrawler set up with this guide https://gist.github.com/dannguyen/f056d05bb7fec408bb7c14ea1552c349

schemacrawler --server=sqlite \
  --info-level=standard \
  --command=schema \
  --output-format=png \
  --database="data/yelpdatabase.sqlite" \
  --output-file="entity_relationship_diagram.png" \
  --portable-names \
  --load-row-counts \
  --title "Yelp Dataset Schema" \ 
  