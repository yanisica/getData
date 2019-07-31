# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 18:35:24 2019

EXTRACTING AMERICAN BIRDS
   
"""

from shapely.geometry import shape, mapping
from shapely.ops import cascaded_union
from shapely.prepared import prep
import fiona
import csv



#Merge african countries

with fiona.open('america.shp') as input:
    # preserve the schema of the original shapefile, including the crs
    meta = input.meta
    with fiona.open('america dissolved.shp', 'w', **input.meta) as output:
        shapes = []
        for country in input:
            shapes.append(shape(country['geometry']))
        dissolved = cascaded_union(shapes)
        output.write({'geometry': mapping(dissolved), 'properties':country['properties']})


with open ('american_birds.csv', 'w', newline = '') as csvfile:
    with fiona.open('jetz_maps.shp') as input:
        with fiona.open ('america dissolved.shp') as america:
            prepared = prep(shape(america[0]['geometry'])) #object that does not change
            for sp in input:
                if prepared.intersects(shape(sp['geometry'])):
                    writer = csv.DictWriter(csvfile, fieldnames={'species': sp['properties']['LATIN']})
                    writer.writerow({'species': sp['properties']['LATIN']})

           
