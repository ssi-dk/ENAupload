#!/usr/bin/env python3

import requests
import io
import random
import xml.etree.ElementTree as ET
import time
import hashlib
import argparse
import os.path
from ftplib import FTP
import re
import json

import EnaSqlite
import GetMetadata

rand = random.Random()

## ENA user variables
with open("user.conf","r") as fh:
    user_variables = json.load(fh)
ena_checklist = "ERC000028"

## ENA url
ena_url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"

## Basic XML forms
submissionxml = dict(add="""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION>
   <ACTIONS>
      <ACTION>
         <ADD/>
      </ACTION>
   </ACTIONS>
</SUBMISSION>
""",
cancel="""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION>
   <ACTIONS>
      <ACTION>
         <CANCEL target="{accession}"/>
      </ACTION>
   </ACTIONS>
</SUBMISSION>
""")

## sqlite setup

dbname = "ena_uploader.sqlite"


class fqfile:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)
        m = hashlib.md5()
        with open(self.path,'rb') as fh:
            m.update(fh.read())
        self.md5 = m.hexdigest()
        self.alias = self.name.split('_')[0]



class Project:
    def __init__(self,
                 alias="myAlias",
                 title="myTitle",
                 description="myDescription",
                 accession=None):
        self.projectxml = """<?xml version = '1.0' encoding = 'UTF-8'?>
        <PROJECT_SET>
           <PROJECT alias="{alias}">
              <TITLE>{title}</TITLE>
              <DESCRIPTION>{description}</DESCRIPTION>
              <SUBMISSION_PROJECT>
                 <SEQUENCING_PROJECT/>
              </SUBMISSION_PROJECT>
           </PROJECT>
        </PROJECT_SET>
"""
        self.alias=alias
        if accession is None:
            accession = db.get_project_accession(alias)
        self.params = {'alias':alias,
                       'title':title,
                       'description':description,
                       'accession':accession}
    def submit(self):
        if args.verbose:
            print("Sending to {}:".format(ena_url))
            print(self.projectxml.format(**self.params))
        r = requests.post(ena_url, files={
            'PROJECT':io.StringIO(
                self.projectxml.format(**self.params)),
            'SUBMISSION':io.StringIO(
                submissionxml['add'])},
                          auth=(user_variables['user'],
                                user_variables['password']))
        if args.verbose:
            print("Received reply:")
            print(r.text)
        self.receipt = ET.fromstring(r.text)
        self.success = self.receipt.get('success')=="true"
        if self.success:
            self.params['accession']=self.receipt.find('PROJECT').get('accession')
            db.add_accession(self.alias, self.params['accession'],
                             'projects')
        return self.success
    
    def cancel(self):
        self.r = requests.post(ena_url, files={
            'SUBMISSION':io.StringIO(submissionxml['cancel'].format(self.params))},
                          auth=(user_variables['user'],
                                user_variables['password']))
        self.receipt = ET.fromstring(r.text)
        return self.receipt.get('success')=="true"
        

class SampleSet:
    def __init__(self):
        self.et = ET.Element("SAMPLE_SET")

    def add_sample(self,alias, title, taxon_id_or_name, sample_attributes):
        db.add_sample(alias)
        sample = ET.SubElement(self.et,"SAMPLE",{'alias':alias})
        sample_name = ET.SubElement(sample, "SAMPLE_NAME")
        if type(taxon_id_or_name) is int or taxon_id_or_name.isnumeric(): 
            sample_name.append(tv_element("TAXON_ID",str(taxon_id_or_name)))
        else:
            sample_name.append(tv_element("SCIENTIFIC_NAME",taxon_id_or_name))
        sampleattributes=ET.SubElement(sample, "SAMPLE_ATTRIBUTES")
        #self.checkmetadata(sample_attributes,ena_checklist)
        for tag,value in sample_attributes.items():
            sample_attribute = ET.SubElement(sampleattributes,"SAMPLE_ATTRIBUTE")
            sample_attribute.append(tv_element("TAG",tag))
            sample_attribute.append(tv_element("VALUE",value))

    def checkmetadata(self,sample_attributes,checklist):
        r = requests.get("https://www.ebi.ac.uk/ena/data/view/{checklist}&display=xml".format(checklist))
        checklistxml = ET.fromstring(r.text)
        for field in checklistxml.iterfind("FIELD"):
            field_name = field.find('NAME').text
            if field_name in sample_attributes.keys():
                field_type = field.find("FIELD_TYPE")[0]
                if field_type.tag == "TEXT_FIELD":
                    regex = field.find('REGEX_VALUE')
                    if regex and re.fullmatch(regex.text,sample_attributes[field_name]) is None:
                        print("{field_name}:\"{value}\" does not match regex \"{}\"".format(field_name,sample_attributes[field_name],regex.text))
                        return False
                elif field_type.tag == "TEXT_CHOICE_FIELD":
                    found = False
                    for value in field_type.finditer("VALUE"):
                        if value.text == sample_attributes[field_name]:
                            found = True
                            break
                    if found == False:
                        return False
            else:
                if field.find('MANDATORY').text == "mandatory":
                    return False
                    
                    
    def submit(self):
        if args.verbose:
            print("Sending to {}:".format(ena_url))
            print(ET.tostring(self.et,encoding="unicode"))

        r = requests.post(ena_url, files={
            'SAMPLE':io.StringIO(ET.tostring(self.et,encoding="unicode")),
            'SUBMISSION':io.StringIO(submissionxml['add'])},
                          auth=(user_variables['user'],
                                user_variables['password']))
        if args.verbose:
            print("Received reply:")
            print(r.text)
        self.receipt = ET.fromstring(r.text)
        self.success = self.receipt.get('success')=="true"
        if self.success:
            self.samples = self.receipt.findall("SAMPLE")
            for sample in self.samples:
                db.add_accession(sample.get('alias'),
                                 sample.get('accession'),
                                 'samples')
        return self.success


class ExperimentSet:
    def __init__(self, project, center_name):
        self.et = ET.Element("EXPERIMENT_SET")
        self.project = project
        self.center_name = center_name

    def add_experiment(self,alias):
        db.add_experiment("exp_"+alias, self.project, alias)
        experiment = ET.SubElement(self.et, "EXPERIMENT",
                                   {'alias': "exp_"+alias,
                                    'center_name': self.center_name})
        study_ref = ET.SubElement(
            experiment, "STUDY_REF", {'refname':self.project})
        design = ET.SubElement(experiment, "DESIGN")
        ET.SubElement(design,"DESIGN_DESCRIPTION")
        sample_ref = ET.SubElement(
            design,"SAMPLE_DESCRIPTOR", {'refname':alias})
        design.append(GetMetadata.getlibrary(alias))
        experiment.append(GetMetadata.getplatform(alias))
        
    def submit(self):
        if args.verbose:
            print("Sending to {}:".format(ena_url))
            print(ET.tostring(self.et,encoding="unicode"))
        r = requests.post(ena_url, files={
            'EXPERIMENT':io.StringIO(ET.tostring(self.et,encoding="unicode")),
            'SUBMISSION':io.StringIO(submissionxml['add'])},
                          auth=(user_variables['user'],
                                user_variables['password']))
        if args.verbose:
            print("Received reply:")
            print(r.text)
        self.receipt = ET.fromstring(r.text)
        self.success = self.receipt.get('success')=="true"
        if self.success:
            self.experiments = self.receipt.findall("EXPERIMENT")
            for experiment in self.experiments:
                db.add_accession(experiment.get('alias'),
                                 experiment.get('accession'),
                                 'experiments')
        return self.success

class RunSet:
    def __init__(self, center_name):
        self.et = ET.Element("RUN_SET")
        self.center_name = center_name

    def add_run(self,alias, file_list):
        db.add_run("run_"+alias, "exp_"+alias)
        run = ET.SubElement(self.et, "RUN",
                            {'alias': "run_"+alias,
                             'center_name': self.center_name})
        experiment_ref = ET.SubElement(
            run, "EXPERIMENT_REF", {'refname':"exp_"+alias})
        data_block = ET.SubElement(run,"DATA_BLOCK")
        files = ET.SubElement(data_block,"FILES")
        for f in file_list:
            ET.SubElement(files, "FILE", {'filename':f.path,
                                          'filetype':"fastq",
                                          'checksum_method':'MD5',
                                          'checksum':f.md5})
        
    def submit(self):
        if args.verbose:
            print("Sending to {}:".format(ena_url))
            print(ET.tostring(self.et,encoding="unicode"))
        r = requests.post(ena_url, files={
            'RUN':io.StringIO(ET.tostring(self.et,encoding="unicode")),
            'SUBMISSION':io.StringIO(submissionxml['add'])},
                          auth=(user_variables['user'],
                                user_variables['password']))
        if args.verbose:
            print("Received reply:")
            print(r.text)
        self.receipt = ET.fromstring(r.text)
        self.success = self.receipt.get('success')=="true"
        if self.success:
            self.runs = self.receipt.findall("RUN")
            for run in self.runs:
                db.add_accession(run.get('alias'),
                                 run.get('accession'),
                                 'runs')
        return self.success


def tv_element(tag,value):
    element = ET.Element(tag)
    element.text = str(value)
    return element

class enaftp:
    def __init__(self, url="webin.ebi.ac.uk"):
        self.url = url

    def connect(self):
        self.ftp = FTP(self.url)
        self.ftp.login(
            user_variables['user'],
            user_variables['password'])

    def upload(self, f):
        with open(f.path,'rb') as fh:
            if args.verbose:
                print("uploading {} to {}".format(f.name,self.url))
            self.ftp.storbinary('STOR fastqs/{}'.format(f.name), fh)
            
    def disconnect(self):
        self.ftp.quit()

def parse_config_file(config_file):
    files = list()
    for line in config_file:
        if line.startswith('#'):
            continue
        files.append(list(map(fqfile,line.strip().split("\t"))))
    return files
    
def parse_arguments():
    parser=argparse.ArgumentParser(description="Upload to ENA")
    parser.add_argument('config_file',type=argparse.FileType()) 
    parser.add_argument('--new-project',action="store_true",help="Create a new project")
    parser.add_argument('-v','--verbose',action="store_true",help="Verbose output")
    parser.add_argument('--no-fastq',action='store_true', help="Don't upload fastqs")
    parser.add_argument('--project', type=str,help="Project name",
                        default = "SSI_"+str(time.localtime().tm_year))
    return parser.parse_args()

if __name__ == "__main__":

    args = parse_arguments()
    
    ## Create DB
    db = EnaSqlite.Sqlitedb(dbname)
    project_alias = args.project
    db.add_project(project_alias)
    
    ## While running in dev mode we don't trust the DB, since everything disappears after 24h
    ## Create a study
    project = Project(project_alias)
    if args.new_project:
        if project.submit():
            db.add_project_accession(project.params['alias'], project.params['accession'])
        else:
            try:
                print("Project submission failed, got accession \"{accession}\" from db".format(
                    project.params))
            except KeyError:
                print("Project submission failed and no accession available in db")
                print(ET.tostring(project.receipt).decode())

    ## Submit samples
    samples = SampleSet()
    experiments = ExperimentSet(project.alias, user_variables['centre_name'])
    runs = RunSet(user_variables['centre_name'])
    files = parse_config_file(args.config_file)
    if not args.no_fastq:
        ftp = enaftp()
        ftp.connect()
        for pair in files:
            for f in pair:
                ftp.upload(f)
        ftp.disconnect()
    for pair in files:
        alias = pair[0].alias
        samples.add_sample(alias,
                           alias,
                           28901,
                           GetMetadata.getmetadata(alias))
        experiments.add_experiment(alias)
        runs.add_run(alias,pair)
    print("Submitting samples: success={}".format(samples.submit()))
    print("Submitting experiments: success={}".format(experiments.submit()))
    print("Submitting runs: success={}".format(runs.submit()))

    db.commit()
    


