import argparse
import csv
import sys
import copy
from datetime import datetime
from io import StringIO
from dgh import CsvDGH


_DEBUG = True


class _Table:

    def __init__(self, pt_path: str, dgh_paths: dict):
        self.table = None
        self.attributes = dict()
        self._init_table(pt_path)
        self.dghs = dict()
        for attribute in dgh_paths:
            self._add_dgh(dgh_paths[attribute], attribute)

    def __del__(self):

        self.table.close()

    def anonymize(self, qi_names: list, k: int, output_path: str, v=True):

        global _DEBUG

        if v:
            _DEBUG = False

        self._debug("[DEBUG] Creating the output file...", _DEBUG)
        try:
            output = open(output_path, 'w')
        except IOError:
            raise
        self._log("[LOG] Created output file.", endl=True, enabled=v)

        self.table.seek(0)

        self._debug(
            "[DEBUG] Instantiating the QI frequency dictionary...", _DEBUG)
        qi_frequency = dict()

        self._debug(
            "[DEBUG] Instantiating the attributes domains dictionary...", _DEBUG)
        domains = dict()
        for i, attribute in enumerate(qi_names):
            domains[i] = set()
        gen_levels = dict()
        for i, attribute in enumerate(qi_names):
            gen_levels[i] = 0

        for i, row in enumerate(self.table):

            qi_sequence = self._get_values(row, qi_names, i)

            if qi_sequence is None:
                self._debug("[DEBUG] Ignoring row %d with values '%s'..." % (i, row.strip()),
                            _DEBUG)
                continue
            else:
                qi_sequence = tuple(qi_sequence)

            if qi_sequence in qi_frequency:
                occurrences = qi_frequency[qi_sequence][0] + 1
                rows_set = qi_frequency[qi_sequence][1].union([i])
                qi_frequency[qi_sequence] = (occurrences, rows_set)
            else:
                qi_frequency[qi_sequence] = (1, set())
                qi_frequency[qi_sequence][1].add(i)

                for j, value in enumerate(qi_sequence):
                    domains[j].add(value)

            self._log("[LOG] Read line %d from the input file." %
                      i, endl=False, enabled=v)

        self._log('', endl=True, enabled=v)

        while True:

            count = 0

            for qi_sequence in qi_frequency:

                if qi_frequency[qi_sequence][0] < k:
                    count += qi_frequency[qi_sequence][0]
            self._debug(
                "[DEBUG] %d tuples are not yet k-anonymous..." % count, _DEBUG)
            self._log("[LOG] %d tuples are not yet k-anonymous..." %
                      count, endl=True, enabled=v)

            if count > k:

                max_cardinality, max_attribute_idx = 0, None
                for attribute_idx in domains:
                    if len(domains[attribute_idx]) > max_cardinality:
                        max_cardinality = len(domains[attribute_idx])
                        max_attribute_idx = attribute_idx

                attribute_idx = max_attribute_idx
                self._debug("[DEBUG] Attribute with most distinct values is '%s'..." %
                            qi_names[attribute_idx], _DEBUG)
                self._log("[LOG] Current attribute with most distinct values is '%s'." %
                          qi_names[attribute_idx], endl=True, enabled=v)

                domains[attribute_idx] = set()
                generalizations = dict()

                for j, qi_sequence in enumerate(list(qi_frequency)):

                    self._log("[LOG] Generalizing attribute '%s' for sequence %d..." %
                              (qi_names[attribute_idx], j), endl=False, enabled=v)

                    if qi_sequence[attribute_idx] in generalizations:
                        generalized_value = generalizations[attribute_idx]
                    else:
                        self._debug(
                            "[DEBUG] Generalizing value '%s'..." % qi_sequence[attribute_idx],
                            _DEBUG)
                        try:
                            generalized_value = self.dghs[qi_names[attribute_idx]]\
                                .generalize(
                                qi_sequence[attribute_idx],
                                gen_levels[attribute_idx])
                        except KeyError as error:
                            self._log('', endl=True, enabled=True)
                            self._log("[ERROR] Value '%s' is not in hierarchy for attribute '%s'."
                                      % (error.args[0], qi_names[attribute_idx]),
                                      endl=True, enabled=True)
                            output.close()
                            return

                        if generalized_value is None:
                            continue

                        generalizations[attribute_idx] = generalized_value

                    new_qi_sequence = list(qi_sequence)
                    new_qi_sequence[attribute_idx] = generalized_value
                    new_qi_sequence = tuple(new_qi_sequence)

                    if new_qi_sequence in qi_frequency:
                        occurrences = qi_frequency[new_qi_sequence][0] \
                            + qi_frequency[qi_sequence][0]
                        rows_set = qi_frequency[new_qi_sequence][1]\
                            .union(qi_frequency[qi_sequence][1])
                        qi_frequency[new_qi_sequence] = (occurrences, rows_set)
                        qi_frequency.pop(qi_sequence)
                    else:
                        qi_frequency[new_qi_sequence] = qi_frequency.pop(
                            qi_sequence)

                    domains[attribute_idx].add(qi_sequence[attribute_idx])

                self._log('', endl=True, enabled=v)

                gen_levels[attribute_idx] += 1

                self._log("[LOG] Generalized attribute '%s'. Current generalization level is %d." %
                          (qi_names[attribute_idx],
                           gen_levels[attribute_idx]), endl=True,
                          enabled=v)

            else:

                self._debug(
                    "[DEBUG] Suppressing max k non k-anonymous tuples...")
                qi_frequency_copy = copy.deepcopy(qi_frequency)
                for qi_sequence, data in qi_frequency.items():
                    if data[0] < k:
                        qi_frequency.pop(qi_sequence)
                self._log("[LOG] Suppressed %d tuples." %
                          count, endl=True, enabled=v)

                self.table.seek(0)

                self._debug("[DEBUG] Writing the anonymized table...", _DEBUG)
                self._log("[LOG] Writing anonymized table...",
                          endl=True, enabled=v)
                for i, row in enumerate(self.table):

                    self._debug(
                        "[DEBUG] Reading row %d from original table..." % i, _DEBUG)
                    table_row = self._get_values(row, list(self.attributes), i)

                    if table_row is None:
                        self._debug("[DEBUG] Skipped reading row %d from original table..." % i,
                                    _DEBUG)
                        continue

                    for qi_sequence in qi_frequency:
                        if i in qi_frequency[qi_sequence][1]:
                            line = self._set_values(
                                table_row, qi_sequence, qi_names)
                            self._debug("[DEBUG] Writing line %d from original table to anonymized "
                                        "table..." % i, _DEBUG)
                            print(line, file=output, end="")
                            break

                break

        output.close()

        self._log("[LOG] All done.", endl=True, enabled=v)

    @staticmethod
    def _log(content, enabled=True, endl=True):
        if enabled:
            if endl:
                print(content)
            else:
                sys.stdout.write('\r' + content)

    @staticmethod
    def _debug(content, enabled=False):

        if enabled:
            print(content)

    def _init_table(self, pt_path: str):

        try:
            self.table = open(pt_path, 'r')
        except FileNotFoundError:
            raise

    def _get_values(self, row: str, attributes: list, row_index=None):
        if row.strip() == '':
            return None

    def _set_values(self, row, values, attributes: list) -> str:
        pass

    def _add_dgh(self, dgh_path: str, attribute: str):
        pass


class CsvTable(_Table):

    def __init__(self, pt_path: str, dgh_paths: dict):

        super().__init__(pt_path, dgh_paths)

    def __del__(self):

        super().__del__()

    def anonymize(self, qi_names, k, output_path, v=False):

        super().anonymize(qi_names, k, output_path, v)

    def _init_table(self, pt_path):

        super()._init_table(pt_path)

        try:
            csv_reader = csv.reader(StringIO(next(self.table)))
        except IOError:
            raise

        for i, attribute in enumerate(next(csv_reader)):
            self.attributes[attribute] = i

    def _get_values(self, row: str, attributes: list, row_index=None):

        super()._get_values(row, attributes, row_index)

        if row_index is not None and row_index == 0:
            return None

        try:
            csv_reader = csv.reader(StringIO(row))
        except IOError:
            raise
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

        try:
            self.dghs[attribute] = CsvDGH(dgh_path)
        except FileNotFoundError:
            raise
        except IOError:
            raise


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Python implementation of the Datafly algorithm. Finds a k-anonymous "
                    "representation of a table.")
    parser.add_argument("--private_table", "-pt", required=True,
                        type=str, help="Path to the CSV table to K-anonymize.")
    parser.add_argument("--quasi_identifier", "-qi", required=True,
                        type=str, help="Names of the attributes which are Quasi Identifiers.",
                        nargs='+')
    parser.add_argument("--domain_gen_hierarchies", "-dgh", required=True,
                        type=str, help="Paths to the generalization files (must have same order as "
                                       "the QI name list.",
                        nargs='+')
    parser.add_argument("-k", required=True,
                        type=int, help="Value of K.")
    parser.add_argument("--output", "-o", required=True,
                        type=str, help="Path to the output file.")
    args = parser.parse_args()

    try:

        start = datetime.now()

        dgh_paths = dict()
        for i, qi_name in enumerate(args.quasi_identifier):
            dgh_paths[qi_name] = args.domain_gen_hierarchies[i]
        table = CsvTable(args.private_table, dgh_paths)
        try:
            table.anonymize(args.quasi_identifier, args.k, args.output, v=True)
        except KeyError as error:
            if len(error.args) > 0:
                _Table._log("[ERROR] Quasi Identifier '%s' is not valid." % error.args[0],
                            endl=True, enabled=True)
            else:
                _Table._log(
                    "[ERROR] A Quasi Identifier is not valid.", endl=True, enabled=True)

        end = (datetime.now() - start).total_seconds()
        _Table._log("[LOG] Done in %.2f seconds (%.3f minutes (%.2f hours))" %
                    (end, end / 60, end / 60 / 60), endl=True, enabled=True)

    except FileNotFoundError as error:
        _Table._log("[ERROR] File '%s' has not been found." % error.filename,
                    endl=True, enabled=True)
    except IOError as error:
        _Table._log("[ERROR] There has been an error with reading file '%s'." % error.filename,
                    endl=True, enabled=True)
