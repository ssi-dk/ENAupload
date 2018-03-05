#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import json
import sys

with open("dk_to_en_country.json",'r') as fh:
    dk_to_en_country = json.load(fh)

class metadata:
    def __init__(self, isolatefile, checklist):
        self.metadata =  {'ENA_CHECKLIST':checklist,
                'collected_by':'Statens Serum Institute',
                'geographic location':'Denmark',
                'geographic location (latitude)':'99999',
                'geographic location (longitude)':'99999',
                'environmental_sample':'no',
                'host health state':'not collected',
                'host scientific name':'Homo Sapiens',
                'Is the sequenced pathogen host associated?':'Yes'}
        self.parse_isolatefile(isolatefile)
    
    def parse_isolatefile(self,isolatefile):
        header = next(isolatefile).split('\t') # headers from BN fields
        isolate_metadata = dict()
        for line in isolatefile:
            fields = line.strip('\n').split('\t')
            isolate = fields[0]
            try:
                isolate_metadata[isolate]={'isolate':isolate,
                                           'serovar':fields[1],
                                           'sub_type':"ST"+fields[2],
                                           'collection date':"{2}-{1}".format(*fields[3].split("-"))}
            except IndexError:
                print(fields)
                sys.exit
            if fields[4]=="DANMARK":
                isolate_metadata[isolate].update({'travel-relation':"not travel-related"})
            elif fields[4]=="Uoplyst" or fields[4]=='':
                isolate_metadata[isolate].update({'travel-relation':"not ascertainable"})
            elif fields[4]=="JA, MEN LAND UKENDT":
                isolate_metadata[isolate].update({'travel-relation':"travel-related"})
            else:
                isolate_metadata[isolate].update({'travel-relation':"travel-related",
                                                  'Country of travel':dk_to_en_country[fields[4]]})
        self.isolate_metadata = isolate_metadata
        return self.isolate_metadata

    def get(self, isolateid):
        result = dict()
        result.update(self.metadata)
        result.update(self.isolate_metadata[isolateid])
        return result

def getmetadata(isolateid, checklist, isolate_attributes):
    ## Default sample attributes
    metadata = {'ENA_CHECKLIST':checklist,
                'collected_by':'Statens Serum Institute',
                'geographic location':'Denmark',
                'geographic location (latitude)':'99999',
                'geographic location (longitude)':'99999',
                'environmental_sample':'no',
                'host health state':'not collected',
                'host scientific name':'Homo Sapiens',
                'Is the sequenced pathogen host associated?':'Yes'}
    

def getlibrary(isolateid):
    params = {'insertsize':500,'insertsd':200}
    library_description = """<LIBRARY_DESCRIPTOR>
     <LIBRARY_NAME/>
     <LIBRARY_STRATEGY>WGS</LIBRARY_STRATEGY>
     <LIBRARY_SOURCE>GENOMIC</LIBRARY_SOURCE>
     <LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
     <LIBRARY_LAYOUT>
         <PAIRED NOMINAL_LENGTH="{insertsize}"  NOMINAL_SDEV="{insertsd}"/>
     </LIBRARY_LAYOUT>
     <LIBRARY_CONSTRUCTION_PROTOCOL>Library was prepared using Illumina Nextera XT Kit.
     </LIBRARY_CONSTRUCTION_PROTOCOL>
 </LIBRARY_DESCRIPTOR>"""
    return ET.fromstring(library_description.format(**params))

def getplatform(isolateid):
    params = {'platform':"Illumina Nextseq 500"}
    platform = """<PLATFORM>
    <ILLUMINA>
         <INSTRUMENT_MODEL>{platform}</INSTRUMENT_MODEL>
    </ILLUMINA>
    </PLATFORM>"""
    return ET.fromstring(platform.format(**params))
