# FileMakerXMLReportParser

Python class to parse Filemaker XML Report to Pandas DataFrames.


### FileMaker Database Design Report (DDR) 

The DDR XML format is useful for advanced users and developers who want to create tools that analyze or process the structure of databases.

*See Filemaker documentation*:
* [FileMaker Pro 18 Advanced Database Design Report XML Output Grammar](https://fmhelp.filemaker.com/docs/18/en/ddrxml/)
* [FileMaker Pro 18 Documentation - Documenting database schemas](https://fmhelp.filemaker.com/help/18/fmp/en/index.html#page/FMP_Help/documenting-schemas.html)


### Usage

```
from filemaker_xml_report_parser import FileMakerXMLReportParser

# Init class 
file_report = FileMakerXMLReportParser("path/to/file.xml")

# Show File Report parsed DataFrames descriptions
file_report.print_dataframes_description()

# External files DataFrame
file_report.df_files

# BaseTables DataFrame
file_report.df_base_tables

...
```

<br>
If there is a related external file that has to be changed, use `print_report` method to see which relationships, calculation/summary fields, layouts or scripts could be afected. This method prints and returns a tuple of filtered DataFrames.


### 

```
dfs_external_file = file_report.print_dataframes_description("external_file")
```