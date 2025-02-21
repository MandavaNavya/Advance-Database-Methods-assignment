
# This will create a local file to run your MapReduce program  

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.util import log_to_stream, log_to_null
from mr3px.csvprotocol import CsvProtocol
import csv 
import operator
import logging


log = logging.getLogger(__name__)
# 
#  Below is the skeleton for a MapReduce program in mrjob.
#  Write your own solution here. Be sure that it actually runs successfully.

class MyMRJob1(MRJob):
    
    
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):  
        log_to_stream(name='mrjob', debug=verbose, stream=stream)
        log_to_stream(name='__main__', debug=verbose, stream=stream)

    def mapper_prodcount(self, _, line):
        result = next(csv.reader([line],quotechar=None)) # extract columns from line

        garment_group_name = result[23]
        prod_name = result[2]
        # skip sparce entries for other columns 
        if prod_name == "prod_name" or prod_name == "" or garment_group_name == "": #skip sparse entries and header
            return
        yield (garment_group_name,prod_name), 1
       


     # The reducer now creates a dict for all department_names, product_names and sections
     # and in the end returns the most or second most frequent values based on its contents
    def reducer_prodcount(self,garmetProd,valuelist):
        garmet, prod = garmetProd
        output = sum(valuelist)
        yield None,(garmet,prod,output)


    def steps(self):
        return [
            MRStep(mapper=self.mapper_prodcount,
                   reducer=self.reducer_prodcount)            
        ]

if __name__ == '__main__':
    MyMRJob1.run()
