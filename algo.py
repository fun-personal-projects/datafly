import argparse
import csv
import sys
import copy
from datetime import datetime
from io import StringIO
from dgh import CsvDGH


class Table:
    def __init__(self, pt_path, dgh_paths):
        self.table = None
        self.attributes = dict()
        self._init_table(pt_path)
        self.dghs = dict()
        for attribute in dgh_paths:
            self._add_dgh(dgh_paths[attribute], attribute)

    def __del__(self):
        self.table.close()

    def anonymize(self, qi_names, k, output_path, v):
        output = open(output_path, 'w')
        print("[LOG] Created output")
        
        self.table.seek(0)
        freqQI = dict()
        domains = dict()
        levels = dict()
        for i, attribute in enumerate(qi_names):
            domains[i] = set()
        for i, attribute in enumerate(qi_names):
            levels[i] = 0 
        print("[LOG] Added qi vals")

        for i, row in enumerate(self.table):
            qiVals = self._get_values(row, qi_names, i)
            if qiVals is None:
                continue
            else:
                qiVals = tuple(qiVals)
                
            if qiVals in freqQI:
                occurrences = freqQI[qiVals][0] + 1
                rows_set = freqQI[qiVals][1].union([i])
                freqQI[qiVals] = (occurrences, rows_set)
            else:
                freqQI[qiVals] = (1, set())
                freqQI[qiVals][1].add(i)
                for j, value in enumerate(qiVals):
                    domains[j].add(value.strip())
                    
        print("[LOG] Got lines from file and added to dict")
        # print(domains)

        while True:
            count = 0
            for qiVals in freqQI:
                if freqQI[qiVals][0] < k:
                    count += freqQI[qiVals][0]
                    
            if count > k:
                print("Some greater k")
                max_cardinality, max_attrPos = 0, None
                for attrPos in domains:
                    if len(domains[attrPos]) > max_cardinality:
                        max_cardinality = len(domains[attrPos])
                        max_attrPos = attrPos

                attrPos = max_attrPos
                domains[attrPos] = set()
                generalizedVals = dict()
                
                for j, qiVals in enumerate(list(freqQI)):

                    if qiVals[attrPos] in generalizedVals:
                        generalized_value = generalizedVals[attrPos]
                    else:
                        generalized_value = self.dghs[qi_names[attrPos]]\
                                .generalize(
                                qiVals[attrPos],
                                levels[attrPos])

                        if generalized_value is None:
                            continue

                        generalizedVals[attrPos] = generalized_value
                    qiModified = list(qiVals)
                    qiModified[attrPos] = generalized_value
                    qiModified = tuple(qiModified)

                    if qiModified in freqQI:
                        occurrences = freqQI[qiModified][0] \
                            + freqQI[qiVals][0]
                        rows_set = freqQI[qiModified][1]\
                            .union(freqQI[qiVals][1])
                        freqQI[qiModified] = (occurrences, rows_set)
                        freqQI.pop(qiVals)
                    else:
                        freqQI[qiModified] = freqQI.pop(
                            qiVals)

                    domains[attrPos].add(qiVals[attrPos])
                    
                levels[attrPos] += 1
            else:
                print("[LOG] Some not suppressed, trying")
                freqQI_copy = copy.deepcopy(freqQI)
                for qiVals, data in freqQI.items():
                    if data[0] < k:
                        freqQI.pop(qiVals)
                self.table.seek(0)

                print("[LOG] Saving new table")
                for i, row in enumerate(self.table):
                    table_row = self._get_values(row, list(self.attributes), i)

                    if table_row is None:
                        continue

                    for qiVals in freqQI:
                        if i in freqQI[qiVals][1]:
                            line = self._set_values(
                                table_row, qiVals, qi_names)
                            print(line, file=output, end="")
                            break
                break
        output.close()
        print("[LOG] Saved")

    def _init_table(self, pt_path):

        self.table = open(pt_path, 'r')

    def _get_values(self, row, attributes, row_index=None):
        if row.strip() == '':
            return None

    def _set_values(self, row, values, attributes):
        pass

    def _add_dgh(self, dgh_path, attribute):
        pass 


class CsvTable(Table):
    def __init__(self, pt_path, dgh_paths):
        super().__init__(pt_path, dgh_paths)
        
    def __del__(self):
        super().__del__()
        
    def anonymize(self, qi_names, k, output_path, v=False):
        super().anonymize(qi_names, k, output_path, v)
        
    def _init_table(self, pt_path):
        super()._init_table(pt_path)
        csv_reader = csv.reader(StringIO(next(self.table)))
        for i, attribute in enumerate(next(csv_reader)):
            self.attributes[attribute] = i

    def _get_values(self, row: str, attributes: list, row_index=None):
        super()._get_values(row, attributes, row_index)
        if row_index is not None and row_index == 0:
            return None
        csv_reader = csv.reader(StringIO(row))
        parsed_row = next(csv_reader)
        values = list()
        for attribute in attributes:
            if attribute in self.attributes:
                values.append(parsed_row[self.attributes[attribute]])
            else:
                raise KeyError(attribute)
        return values

    def _set_values(self, row: list, values, attributes: list):
        for i, attribute in enumerate(attributes):
            row[self.attributes[attribute]] = values[i]
        values = StringIO()
        csv_writer = csv.writer(values)
        csv_writer.writerow(row)
        return values.getvalue()

    def _add_dgh(self, dgh_path, attribute):
        self.dghs[attribute] = CsvDGH(dgh_path)

# Get all the required arguments
parser = argparse.ArgumentParser()
parser.add_argument("--private_table", "-pt", required=True,
                    type=str, help="Path to the table")
parser.add_argument("--quasi_identifier", "-qi", required=True,
                    type=str, help="QI vals",
                    nargs='+')
parser.add_argument("--domain_gen_hierarchies", "-dgh", required=True,
                    type=str, help="DGH files",
                    nargs='+')
parser.add_argument("-k", required=True,
                    type=int, help="K")
parser.add_argument("--output", "-o", required=True,
                    type=str, help="Path to the output ")
args = parser.parse_args()

# Apply the DGH to everything
try:
    dgh_paths = dict()
    for i, qi_name in enumerate(args.quasi_identifier):
        dgh_paths[qi_name] = args.domain_gen_hierarchies[i]
    table = CsvTable(args.private_table, dgh_paths)
    try:
        table.anonymize(args.quasi_identifier, args.k, args.output, v=True)
    except KeyError as error:
        if len(error.args) > 0:
            print(f"QI {error.args[0]} is invalid")
        else:
            print("[LOG] A QI is not valid")
except FileNotFoundError as error:
    print("[LOG] Check file path")
except IOError as error:
    print("[LOG] File corrupted")
