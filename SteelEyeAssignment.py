#!/usr/bin/env python3

#Author - Yuvaraja Subramaniam ( https://www.linkedin.com/in/yuvaraja )

from urllib.request import urlopen
from xml.dom import minidom
from lxml import etree
import zipfile
import pandas as pd


#URL to download the xml file from
url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'


# xml file to download into
xmlfile = 'download.xml'


# DOWNLOAD XML

def download_xml(url, xmlfile):
    #fire the http request and download the file
    xml = open(xmlfile, "w+")
    xml.write(urlopen(url).read().decode('utf-8'))
    xml.close()



# GET NODE TEXT

def getNodeText(node):

    nodelist = node.childNodes
    result = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            result.append(node.data)
    return ''.join(result)




# PARSE XML AND GET THE FIRST SUITABLE FILE'S URL LINK

def parse_xml_and_get_ziplink(xmlfile):
    print('Parsing xml file ' + xmlfile + ' to find the first download link whose file_type is DLTINS')

    xmldoc = minidom.parse(xmlfile)
    filelist = []

    doclist = xmldoc.getElementsByTagName('doc')

    for doc in doclist:
        strings_list = doc.getElementsByTagName('str')

        linkname = ''
        filename = ''
        filetype = ''

        for mystr in strings_list:

            if mystr.attributes['name'].value == 'download_link' :
                linkname = getNodeText(mystr)
                print ('download_link = ' + linkname)

            if mystr.attributes['name'].value == 'file_name' :
                filename = getNodeText(mystr)
                print ('file_name = ' + filename)

            if mystr.attributes['name'].value == 'file_type' :
                filetype = getNodeText(mystr)
                print ('file_type = ' + filetype)

        filelist.append({'filename':filename, 'filetype':filetype, 'download_link':linkname})
    
    #return the link for the first file of type DLTINS
    for filedict in filelist :
        if filedict['filetype'] == 'DLTINS' :
            return (filedict['filename'], filedict['download_link'])



# DOWNLOAD THE ZIP FILE

def download_zip(url, zipfilename):
    #fire the http request and download the file
    zipf = open(zipfilename, "wb")
    zipf.write(urlopen(url).read())
    zipf.close()



# CREATE CSV

def create_csv(datafile, csvfile):
    print('Parsing data xml file ' + datafile)

    FinInstrmGnlAttrbts_Id = []
    FinInstrmGnlAttrbts_FullNm = []
    FinInstrmGnlAttrbts_ClssfctnTp = []
    FinInstrmGnlAttrbts_CmmdtyDerivInd = []
    FinInstrmGnlAttrbts_NtnlCcy = []
    Issr = []

    data_Id = ''
    data_FullNm = ''
    data_ClssfctnTp = ''
    data_CmmdtyDerivInd = ''
    data_NtnlCcy = ''
    data_Issr = ''

    for event, elem in etree.iterparse(datafile):

        #print ("Processing element tag = " + elem.tag)

        if elem.tag.endswith('}FinInstrmGnlAttrbts') :

            for child in elem:
                if child.tag.endswith("}Id"):
                    data_Id = child.text

                elif child.tag.endswith("}FullNm"):
                    data_FullNm = child.text

                elif child.tag.endswith("}ClssfctnTp"):
                    data_ClssfctnTp = child.text

                elif child.tag.endswith("}CmmdtyDerivInd"):
                    data_CmmdtyDerivInd = child.text

                elif child.tag.endswith("}NtnlCcy"):
                    data_NtnlCcy = child.text

            elem.clear()

        elif elem.tag.endswith("}Issr") :
            data_Issr = elem.text
            FinInstrmGnlAttrbts_Id.append(data_Id)
            FinInstrmGnlAttrbts_FullNm.append(data_FullNm)
            FinInstrmGnlAttrbts_ClssfctnTp.append(data_ClssfctnTp)
            FinInstrmGnlAttrbts_CmmdtyDerivInd.append(data_CmmdtyDerivInd)
            FinInstrmGnlAttrbts_NtnlCcy.append(data_NtnlCcy)
            Issr.append(data_Issr)

            print( data_Id + ',' + data_FullNm + ',' + data_ClssfctnTp + ',' + data_CmmdtyDerivInd + ',' + data_NtnlCcy + ',' + data_Issr )

            data_Id = ''
            data_FullNm = ''
            data_ClssfctnTp = ''
            data_CmmdtyDerivInd = ''
            data_NtnlCcy = ''
            data_Issr = ''
            elem.clear()

    df = pd.DataFrame({
        'FinInstrmGnlAttrbts.Id'            : FinInstrmGnlAttrbts_Id,
        'FinInstrmGnlAttrbts.FullNm'        : FinInstrmGnlAttrbts_FullNm,
        'FinInstrmGnlAttrbts.ClssfctnTp'    : FinInstrmGnlAttrbts_ClssfctnTp,
        'FinInstrmGnlAttrbts.CmmdtyDerivInd': FinInstrmGnlAttrbts_CmmdtyDerivInd,
        'FinInstrmGnlAttrbts_NtnlCcy'       : FinInstrmGnlAttrbts_NtnlCcy
    })

    print ("Saving the dataframe to csv file : " + csvfile)
    df.to_csv( csvfile, index=False )

    #print(df)



# MAIN PROGRAM

def main():
    
    print('SteelEye Assignment Solution')

    # download xmlfile
    download_xml(url, xmlfile)
    print('Downloaded xml file : ' + xmlfile)

    # parse xml file
    (zipfilename, ziplink) = parse_xml_and_get_ziplink(xmlfile)
    print('Parsed xml file : ' + xmlfile)

    print('First DLTINS file name = ' + zipfilename)
    print('First DLTINS file link = ' + ziplink)

    # download the zip file
    download_zip(ziplink, zipfilename)
    print('Downloaded zip file : ' + zipfilename)

    # unzip the zip file
    with zipfile.ZipFile(zipfilename, 'r') as zip_ref:
        zip_ref.extractall('.')

    print('Extracted xml file from zip file...')

    #create the csv file
    datafile = zipfilename
    csvfile  = zipfilename
    datafile = datafile.replace("zip", "xml")
    csvfile  = csvfile.replace("zip", "csv")
    print("Data  xml file = " + datafile)
    print("Data  csv file = " + csvfile)

    create_csv(datafile, csvfile)
    print ("------------------------------------------------------------------------")
    print ("CREATED THE FINAL RESULTS CSV FILE : " + csvfile)
    print ("------------------------------------------------------------------------")

    #TODO AWS S3 bucket storage & AWS Lambda. Need AWS account credentials from SteelEye end to code the remaining stuff


if __name__ == "__main__":

    # calling main function
    main()
