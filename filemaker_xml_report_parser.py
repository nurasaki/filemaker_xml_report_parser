import pandas as pd
from typing import List, Tuple
import pandas as pd
import lxml.etree as ET


# from filemaker_xml_report_parser import FileMakerXMLReportParser

class FileMakerXMLReportParser:

    def __init__(self, xml_file):

        # print("Holi")

        self.xml_file = xml_file

        tree = ET.parse(xml_file)
        root = tree.getroot()

        # BaseTableCatalog {}
        (
            self.df_base_tables,
            self.df_fields,
            self.df_calculated_fields
        ) = self.parse_base_table_catalog(root)

        # RelationshipGraph {}
        (
            self.df_tables,
            self.df_rels,
            self.df_field_joins
        ) = self.parse_relationship_graph(root)

        # LayoutCatalog {}
        (
            self.df_layouts,
            self.df_layout_fields
        ) = self.parse_layout_catalog(root)

        # ScriptCatalog {}
        (
            self.df_scripts,
            self.df_script_steps,
            self.df_script_fields,
            self.df_script_layouts,
            self.df_script_scripts) = self.parse_script_catalog(root)

        # ExternalDataSourcesCatalog
        self.df_files = self.parse_external_data_sources_catalog(root)

        # ValueListCatalog {}
        (
            self.df_value_lists,
            self.df_value_lists_fields
        ) = self.parse_value_list_catalog(root)

    # External Data Sources
    @staticmethod
    def parse_external_data_sources_catalog(root: ET.Element) -> pd.DataFrame:

        """Parses xml_file ExternalDataSourcesCatalog element

        * Returns:
        ----------------------------------------------------------------------------
        - df_files        -> pd.DataFrame (External files references)


        * XML structure:
        ----------------------------------------------------------------------------
        '/File/ExternalDataSourcesCatalog/FileReference'


        * Columns dtypes:
        ----------------------------------------------------------------------------
        - pathList     object
        - file_id       int32
        - file_name    object

        """

        # Parse XML file and get root element
        # tree = ET.parse(xml_file)
        # root = tree.getroot()

        # Init lists
        files = []

        for file in root.find("File/ExternalDataSourcesCatalog"):
            file_dict = dict(file.attrib)
            file_dict['file_id'] = file_dict.pop('id', '')
            file_dict['file_name'] = file_dict.pop('name', '')

            files.append(file_dict)

        # Create DataFrames

        # File scripts
        df_files = pd.DataFrame(files).astype({'file_id': 'int32'})

        return df_files

    # Base Tables
    @staticmethod
    def parse_base_table_catalog(root: ET.Element) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Parses xml_file BaseTableCatalog element:

        * Returns
        --------------------------------------------------------------------------------
        - df_base_tables       -> pd.DataFrame (Base Tables)
        - df_fields            -> pd.DataFrame (Files of base tables)
        - df_calculated_fields -> pd.DataFrame (Referenced fields used in Calculation/Summary fields)


        * XML structure:
        --------------------------------------------------------------------------------
        "File/BaseTableCatalog/BaseTable/FieldCatalog/Field"
        BaseTable {'id': '129', 'records': '163151', 'name': 'Compta'}


        * Returns (DataFrames)
        --------------------------------------------------------------------------------
        Columns (df_base_tables):
        - base_table_id       int32
        - base_table_name    object
        - records             int32

        Columns (df_fields):
        - base_table_id       int32
        - base_table_name    object
        - records             int32
        - field_id            int32
        - field_name         object
        - dataType           object
        - fieldType          object

        Columns (df_calculated_fields):
        - field_id            int32
        - field_name         object
        - dataType           object
        - fieldType          object
        - base_table_id       int32
        - base_table_name    object
        - records             int32
        - ref_field_id        int32
        - ref_field_name     object
        - ref_table_name     object
        """

        # tree = ET.parse(xml_file)
        # root = tree.getroot()

        base_tables = []
        fields = []
        related_fields = []

        for base_table_el in root.find(f"File/BaseTableCatalog"):

            # BaseTable {'id': '129', 'records': '163151', 'name': 'Compta'}
            base_table_dict = dict(base_table_el.attrib)
            base_table_dict['base_table_id'] = base_table_dict.pop('id')
            base_table_dict['base_table_name'] = base_table_dict.pop('name')
            base_tables.append(base_table_dict)

            #  {'id': '175', 'dataType': 'Text', 'fieldType': 'Normal', 'name': 'PrimaryKey'}
            for field in base_table_el.findall("FieldCatalog/Field"):
                field_dict = dict(field.attrib)
                field_dict['field_id'] = field_dict.pop('id')
                field_dict['field_name'] = field_dict.pop('name')
                field_dict.update(base_table_dict)
                fields.append(field_dict)

                # Get Fields used in Calculated fields
                if field_dict['fieldType'] == 'Calculated':

                    # Find All reference Fields
                    for field in field.findall("DisplayCalculation/Chunk/Field"):
                        # Update field dict
                        field_dict.update(dict(field.attrib))
                        field_dict['ref_field_id'] = field_dict.pop('id')
                        field_dict['ref_field_name'] = field_dict.pop('name')
                        field_dict['ref_table_name'] = field_dict.pop('table')

                        # Append to list
                        related_fields.append(field_dict)

                # Get Fields used in Summary fields
                if field_dict['fieldType'] == 'Summary':
                    # Find reference Field
                    field = field.find("SummaryInfo/SummaryField/Field")

                    # Update field dict
                    field_dict.update(dict(field.attrib))
                    field_dict['ref_field_id'] = field_dict.pop('id')
                    field_dict['ref_field_name'] = field_dict.pop('name')

                    # Append to list
                    related_fields.append(field_dict)

                    # Base Tables DataFrame
        df_base_tables = pd.DataFrame(base_tables).astype({
            'records': 'int32',
            'base_table_id': 'int32'
        })

        df_base_tables = df_base_tables[['base_table_id', 'base_table_name', 'records']]

        # Fields DataFrame
        field_cols = ['base_table_id', 'base_table_name', 'records',
                      'field_id', 'field_name', 'dataType', 'fieldType', ]

        df_fields = pd.DataFrame(fields).astype({
            'field_id': 'int32',
            'records': 'int32',
            'base_table_id': 'int32',
        })[field_cols]

        # Calculation/Summary related Fields Data Frame
        df_calculated_fields = pd.DataFrame(related_fields)

        # GroupBy
        group_cols = ['field_id', 'base_table_id', 'ref_field_id', 'ref_table_name']
        df_calculated_fields = df_calculated_fields.groupby(group_cols).first().reset_index()

        rel_fields_cols = ['field_id', 'field_name', 'dataType', 'fieldType',
                           'base_table_id', 'base_table_name', 'records',
                           'ref_field_id', 'ref_field_name', 'ref_table_name']

        # Set dtypes
        df_calculated_fields = df_calculated_fields.astype({
            'field_id': 'int32',
            'records': 'int32',
            'base_table_id': 'int32',
            'ref_field_id': 'int32',
        })[rel_fields_cols]

        return df_base_tables, df_fields, df_calculated_fields

    # Relationships and Field Joins
    @staticmethod
    def parse_relationship_graph(root: ET.Element) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Get DataFrame of Relationships and FieldJoins of relationships

        * XML structure:
        ----------------------------------------------------------------------------
        File{}
            RelationshipGraph{}
                TableList {}
                RelationshipList {}
                    Relationship {'id': '1'}
                        LeftTable {'cascadeCreate': 'False', 'cascadeDelete': 'False', 'name': '_Compta'}
                        RightTable {'cascadeCreate': 'False', 'cascadeDelete': 'False', 'name': 'CLI_Clients'}
                        JoinPredicateList {}
                            JoinPredicate {'type': 'Equal'}
                                LeftField {}
                                RightField {}
                                    Field {'table': '_Compta', 'id': '1', 'name': 'ID'}
                            JoinPredicate {'type': 'Equal'}
                            JoinPredicate {'type': 'Equal'}
                    Relationship {'id': '1'}
                    ...


        * Returns:
        ----------------------------------------------------------------------------
        - df_tables        -> pd.DataFrame (Tables defined in relationships)
        - df_relationships -> pd.DataFrame (Relationships between tables)
        - df_field_joins   -> pd.DataFrame (Fields used in relationships)


        Columns (df_tables):
        - table_id            int32
        - color              object
        - base_table_id       int32
        - base_table_name    object
        - table_name         object

        Columns (df_relationships):
        - relationship_id      int32
        - left_table_name     object
        - right_table_name    object

        Columns (df_field_joins):
        - relationship_id     int32
        - type               object
        - join_side          object
        - table_name         object
        - field_id            int32
        - name               object
        """

        # Pares xml_file and get root element
        # tree = ET.parse(xml_file)
        # root = tree.getroot()

        tables = []
        relations = []
        field_joins = []

        # Get all tables in TableList tag
        for table in root.find(f"File/RelationshipGraph/TableList"):

            table_dict = dict(table.attrib)
            external = table.find("FileReference")

            if external is not None:
                table_dict.update({
                    'external_file_id': external.attrib['id'],
                    'external_file_name': external.attrib['name']
                })

            tables.append(table_dict)

        # Convert dtypes
        df_tables = pd.DataFrame(tables).astype({
            'id': 'int32',
            'baseTableId': 'int32'
        })

        # Rename columns
        df_tables = df_tables.rename(columns={
            'id': 'table_id',
            'baseTableId': 'base_table_id',
            'baseTable': 'base_table_name',
            'name': 'table_name',
        })

        # Get RelationshipList{}
        for rel in root.find(f"File/RelationshipGraph/RelationshipList"):

            relation_dict = {
                'relationship_id': rel.attrib['id'],
                'left_table_name': rel[0].attrib['name'],
                'right_table_name': rel[1].attrib['name'],
            }

            relations.append(relation_dict)

            for join in rel.findall("JoinPredicateList/JoinPredicate"):
                join_dict = dict(join.attrib)

                for field in join:
                    field_dict = dict(field.find("Field").attrib)

                    field_dict['table_name'] = field_dict.pop('table', '')
                    field_dict['field_id'] = field_dict.pop('id', '')
                    field_dict['field_name'] = field_dict.pop('name', '')

                    field_dict.update(join_dict)
                    field_dict.update(relation_dict)

                    field_joins.append(field_dict)

        # print(field_joins)

        # Create Relationships DataFrame
        df_rels = pd.DataFrame(relations).astype({'relationship_id': 'int32'})

        # Rename columns
        df_rels = df_rels.rename(columns={
            'id': 'field_id',
            'table': 'table_name'
        })

        # Create Field Joins DataFrame
        df_field_joins = pd.DataFrame(field_joins).astype({
            'field_id': 'int32',
            'relationship_id': 'int32',
        })

        # Rename columns
        df_field_joins = df_field_joins.rename(columns={
            'id': 'field_id',
            'table': 'table_name'
        })

        return df_tables, df_rels, df_field_joins

    # Layouts
    @staticmethod
    def parse_layout_catalog(root: ET.Element) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Parses xml_file LayoutCatalog element

        * Returns:
        ----------------------------------------------------------------------------
        - df_layouts        -> pd.DataFrame (File layouts)
        - df_layout_fields  -> pd.DataFrame (Fields used in file layouts)


        * XML structure:
        ----------------------------------------------------------------------------

        Layout {}
        '/File/LayoutCatalog/Layout'
        '/File/LayoutCatalog/Group[]/Layout'

        Field {}
        '/File/LayoutCatalog/Layout/Field'


        * Columns dtypes:
        ----------------------------------------------------------------------------

        Columns (df_layouts):
        - layout_id         int32
        - layout_name      object
        - table_id         object
        - table_name       object
        - width             int32
        - quickFind        object
        - includeInMenu    object


        Columns (df_layout_fields):
        - width                int32
        - quickFind           object
        - includeInMenu       object
        - layout_id            int32
        - layout_name         object
        - table_id             int32
        - table_name          object
        - field_table_name    object
        - field_name          object
        """

        # tree = ET.parse(xml_file)
        # root = tree.getroot()

        layouts = []
        fields = []

        for layout in root.findall("File/LayoutCatalog/.//Layout"):
            parent = layout.find('..')
            if parent.tag == "LayoutCatalog" or parent.tag == "Group":

                # print(layout.attrib)

                # Get Assiciated Table
                table = layout.find("Table").attrib
                layout_dict = dict(layout.attrib)

                layout_dict['layout_id'] = layout_dict.pop('id')
                layout_dict['layout_name'] = layout_dict.pop('name')
                layout_dict['table_id'] = table['id']
                layout_dict['table_name'] = table['name']

                # Append layout dict to list
                layouts.append(layout_dict)

                # Get Layout Fields

                for field in layout.findall("Object[@type='Field']/FieldObj/Name"):
                    if field.text is not None:
                        f = field.text.split("::")

                        # field_dict = layout_dict
                        field_dict = layout_dict.copy()
                        field_dict['field_table_name'] = f[0]
                        field_dict['field_name'] = f[1]

                        # print(f[1])
                        fields.append(field_dict)

                        # Layouts DataFrame
        cols = ['layout_id', 'layout_name', 'table_id', 'table_name',
                'width', 'quickFind', 'includeInMenu']
        df_layouts = pd.DataFrame(layouts).astype({'width': 'int32', 'layout_id': 'int32'})[cols]
        #  df_layouts = df_layouts.rename(columns={'id': 'layout_id', 'name': 'layout_name'})

        # Layout Fileds DataFrame
        # cols = ['layout_id', 'layout_name', 'table_id', 'table_name', 'width', 'quickFind', 'includeInMenu']
        df_layout_fields = pd.DataFrame(fields).astype({
            'width': 'int32',
            'layout_id': 'int32',
            'table_id': 'int32'
        })

        return df_layouts, df_layout_fields

    # Scripts
    @staticmethod
    def parse_script_catalog(root: ET.Element) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        """Parses xml_file ScriptCatalog element

        * Returns:
        ----------------------------------------------------------------------------
        - df_scripts        -> pd.DataFrame (File scripts)
        - df_script_steps   -> pd.DataFrame (Steps used in file scripts)
        - df_script_fields  -> pd.DataFrame (Fields used in file scripts/steps)
        - df_script_layouts -> pd.DataFrame (Layouts used in file scripts/steps)
        - df_script_scripts -> pd.DataFrame (Scripts used in file scripts/steps)


        * XML structure:
        ----------------------------------------------------------------------------

        Layout {}
        '/File/ScriptCatalog/Group[9]/Script[14]/StepList/Step[]/Layout'

        Field {}
        '/File/ScriptCatalog/Group[10]/Script/StepList/Step/TargetFields/Field[351]'
        '/File/ScriptCatalog/Group[10]/Script/StepList/Step/Field[351]'

        Script {}
        '/File/ScriptCatalog/Group[]/Script[]/StepList/Step[]/Script'


        * Columns dtypes:
        ----------------------------------------------------------------------------

        Columns (df_scripts):
        - includeInMenu    object
        - runFullAccess    object
        - script_id         int32
        - script_name      object


        Columns (df_script_steps):
        - enable           object
        - step_id           int32
        - step_name        object
        - includeInMenu    object
        - runFullAccess    object
        - script_id         int32
        - script_name      object


        Columns (df_script_fields):
        - table_name                object
        - field_id                   int32
        - field_name                object
        - enable                    object
        - step_id                    int32
        - step_name                 object
        - includeInMenu             object
        - runFullAccess             object
        - script_id                  int32
        - script_name               object
        - map                       object
        - GroupByFieldIsSelected    object


        Columns (df_script_layouts):
        - layout_id         int32
        - layout_name      object
        - enable           object
        - step_id           int32
        - step_name        object
        - includeInMenu    object
        - runFullAccess    object
        - script_id         int32
        - script_name      object


        Columns (df_script_scripts):
        - subscript_id      int32
        - suscript_name    object
        - enable           object
        - step_id           int32
        - step_name        object
        - includeInMenu    object
        - runFullAccess    object
        - script_id         int32
        - script_name      object
        """

        # Parse XML file and get root element
        # tree = ET.parse(xml_file)
        # root = tree.getroot()

        # Init lists
        scripts = []
        script_steps = []
        script_fields = []
        script_layouts = []
        script_scripts = []

        for script in root.findall("File/ScriptCatalog/.//Script"):
            # print(tree.getpath(el)) #el.attrib)
            parent = script.find('..')

            if parent.tag == "ScriptCatalog" or parent.tag == "Group":

                script_dict = dict(script.attrib)
                script_dict['script_id'] = script_dict.pop('id')
                script_dict['script_name'] = script_dict.pop('name')
                scripts.append(script_dict)

                #  Get script Steps
                for step in script.findall("StepList/Step"):

                    step_dict = dict(step.attrib)
                    step_dict['step_id'] = step_dict.pop('id')
                    step_dict['step_name'] = step_dict.pop('name')

                    #  Update/Append Step dict
                    step_dict.update(script_dict)
                    script_steps.append(step_dict)

                    # Get fields used in steps
                    for field in step.findall(".//Field"):
                        field_dict = dict(field.attrib)
                        field_dict['table_name'] = field_dict.pop('table', '')
                        field_dict['field_id'] = field_dict.pop('id', '')
                        field_dict['field_name'] = field_dict.pop('name', '')

                        #  Update/Append Field dict
                        field_dict.update(step_dict)
                        script_fields.append(field_dict)

                        # ToDo
                        #  Analize "Export Records" steps
                        # <Step enable="True" id="36" name="Export Records">

                    # Find used Layouts in scipt steps
                    for layout in step.findall(".//Layout"):
                        if layout.attrib:

                            layout_dict = dict(layout.attrib)
                            layout_dict['layout_id'] = layout_dict.pop('id', '')
                            layout_dict['layout_name'] = layout_dict.pop('name', '')

                            # Find external layout tables
                            table = layout.find('..').find('Table')
                            if table is not None:
                                table_dict = dict(table.attrib)
                                table_dict['table_id'] = table_dict.pop('id', '')
                                table_dict['table_name'] = table_dict.pop('name', '')
                                layout_dict.update(table_dict)

                            #  Update/Append Layout dict
                            layout_dict.update(step_dict)
                            script_layouts.append(layout_dict)

                    # Find used Scripts in scipt steps
                    for sub_script in step.findall(".//Script"):
                        sub_script_dict = dict(sub_script.attrib)
                        sub_script_dict['subscript_id'] = sub_script_dict.pop('id', '')
                        sub_script_dict['suscript_name'] = sub_script_dict.pop('name', '')

                        #  Update/Append Layout dict
                        sub_script_dict.update(step_dict)
                        script_scripts.append(sub_script_dict)

        # Create DataFrames

        # File scripts
        df_scripts = pd.DataFrame(scripts).astype({'script_id': 'int32'})

        # Steps used in file scripts
        df_script_steps = pd.DataFrame(script_steps).astype({
            'script_id': 'int32',
            'step_id': 'int32'})

        # Fields used in file scripts/steps
        df_script_fields = pd.DataFrame(script_fields).astype({
            'script_id': 'int32',
            'step_id': 'int32',
            'field_id': 'int32'})

        # Layouts used in file scripts/steps
        df_script_layouts = pd.DataFrame(script_layouts).astype({
            'script_id': 'int32',
            'step_id': 'int32',
            # 'table_id': 'int32' (ValueError: cannot convert float NaN to integer)
            'layout_id': 'int32', })

        # Scripts used in file scripts/steps
        df_script_scripts = pd.DataFrame(script_scripts).astype({
            'script_id': 'int32',
            'step_id': 'int32',
            'subscript_id': 'int32'})

        return df_scripts, df_script_steps, df_script_fields, df_script_layouts, df_script_scripts

    # Value List
    @staticmethod
    def parse_value_list_catalog(root: ET.Element) -> Tuple[pd.DataFrame, pd.DataFrame]:

        """
        ValueListCatalog

        - df_value_lists         -> pd.DataFrame (File value lists)
        - df_value_lists_fields  -> pd.DataFrame (Fields used in value lists)
        """

        value_lists = []
        value_list_fields = []

        for value_list in root.find("File/ValueListCatalog"):

            value_list_dict = dict(value_list.attrib)
            value_list_dict['value_list_id'] = value_list_dict.pop('id', '')
            value_list_dict['value_list_name'] = value_list_dict.pop('name', '')
            value_list_dict.update(value_list[0].attrib)

            value_lists.append(value_list_dict)

            if value_list_dict['value'] == "Field":

                for field in value_list.findall(".//Field"):
                    parent = field.find("..")

                    field_dict = dict(field.attrib)
                    field_dict["type"] = parent.tag

                    field_dict["table_name"] = field_dict.pop('table', '')
                    field_dict["field_id"] = field_dict.pop('id', '')
                    field_dict["field_name"] = field_dict.pop('name', '')

                    value_list_fields.append(field_dict)

        df_value_lists = pd.DataFrame(value_lists).astype({'value_list_id': 'int32'})
        df_value_lists_fields = pd.DataFrame(value_list_fields).astype({'field_id': 'int32'})

        return df_value_lists, df_value_lists_fields

    @staticmethod
    def print_dataframes_description():

        df_descriptions = {
            "df_files": "External files references",
            "df_base_tables": "Base tables of the file",
            "df_fields":  "Base tables fields",
            "df_calculated_fields": "Referenced fields used in Calculation or Summary fields",
            "df_tables": "Tables defined in relationship graph",
            "df_relationships": "Relationships between tables",
            "df_field_joins": "Fields used in relationships",
            "df_layouts": "File layouts",
            "df_layout_fields": "Fields used in file layouts",
            "df_scripts": "File scripts",
            "df_script_steps": "Steps used in file scripts",
            "df_script_fields": "Fields used in file scripts/steps",
            "df_script_layouts": "Layouts used in file scripts/steps",
            "df_script_scripts": "Scripts used in file scripts/steps",
            "df_value_lists": "File value lists",
            "df_value_lists_fields": "Fields used in value lists"
        }

        # Output string format
        print_fmt = "{:<25}{}"

        # Print header
        print(print_fmt.format('DataFrame', "Description"))
        print("-" * 80)

        # Print DataFrames descriptions
        for df, descr in df_descriptions.items():
            print(print_fmt.format(df, descr))

        # return [df for df in self.__dict__.keys() if df[:3] == 'df_']

    def print_report(self, external_file, print_function=print):
        """Print external file report of:
         * External file fields/base tables defined in tables in relationships graph (df_tables)
         * External file fields used in relationships join (df_field_joins)
         * External file fields used in Calculation/Summary fields (df_calculated_fields)
         * External file fields used in layouts (df_layout_fields)
         * External file fields used in scripts (df_script_fields)
        """
        print(f"External File: {external_file}\n")

        # Tables using ExternalFile
        # ----------------------------------------------------------------------------------------
        print("1. Relationship tables (df_tables)")

        cols = ['external_file_name', 'base_table_name', 'table_name']
        df_tables = self.df_tables[cols][self.df_tables['external_file_name'] == external_file]
        tables = df_tables['table_name']
        print_function(df_tables)
        print("\n")

        # Relationship Fields
        # ----------------------------------------------------------------------------------------
        print("2. Relationship join Fields (df_field_joins)")

        cols = ['relationship_id', 'left_table_name', 'right_table_name', 'table_name', 'field_name']
        df_field_joins = self.df_field_joins[cols][self.df_field_joins['table_name'].isin(tables)]
        print_function(df_field_joins)
        print("\n")

        # Calculated Fields using "File" fields
        # ----------------------------------------------------------------------------------------
        print("3. Calculated Fields (df_calculated_fields)")
        cols = ['base_table_name', 'field_name', 'fieldType', 'ref_field_name', 'ref_table_name']
        mask = self.df_calculated_fields['ref_table_name'].isin(tables)
        df_calculated_fields = self.df_calculated_fields[cols][mask]
        print_function(df_calculated_fields)
        print("\n")

        # Layout Fields
        # ----------------------------------------------------------------------------------------
        print("4. Layout Fields grouped (df_layout_fields_grouped)")
        cols = ['layout_id', 'layout_name', 'field_table_name', 'field_name']
        df_layout_fields = self.df_layout_fields[self.df_layout_fields['field_table_name'].isin(tables)][cols]
        df_layout_fields = df_layout_fields.groupby(["field_table_name", "field_name"]).count()
        df_layout_fields = df_layout_fields.reset_index().rename(columns={'layout_id': 'count_lays'})

        print_function(df_layout_fields)
        print("\n")

        # Script Fields
        # ----------------------------------------------------------------------------------------
        print("5. Script Fields (df_script_fields)")
        cols = ['table_name', 'field_name', 'script_name', 'step_id']

        df_script_fields = self.df_script_fields[self.df_script_fields['table_name'].isin(tables)][cols]
        df_script_fields = df_script_fields[cols].groupby(cols[:-1]).count().reset_index()
        df_script_fields = df_script_fields.rename(columns={'step_id': 'step_count'})
        print_function(df_script_fields)
        print("\n")

        return df_tables, df_field_joins, df_calculated_fields, df_layout_fields, df_script_fields

