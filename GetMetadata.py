#!/usr/bin/env python3

import xml.etree.ElementTree as ET


def getmetadata(isolateid):
    return {'collection_date':2010,'ENA_CHECKLIST':"ERC000014"}

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
