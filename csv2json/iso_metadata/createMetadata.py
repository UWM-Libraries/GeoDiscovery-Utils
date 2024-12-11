import xml.etree.ElementTree as ET
import os
import csv
import numpy
from datetime import datetime
from time import gmtime, strftime

#Map and register namespaces

namespaces = {'gmd': 'http://www.isotc211.org/2005/gmd','gco': 'http://www.isotc211.org/2005/gco', 'gml': 'http://www.opengis.net/gml', 'gfc': 'http://www.isotc211.org/2005/gfc'}
ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
ET.register_namespace('gmd', 'http://www.isotc211.org/2005/gmd')
ET.register_namespace('gco', 'http://www.isotc211.org/2005/gco')
ET.register_namespace('gts', 'http://www.isotc211.org/2005/gts')
ET.register_namespace('gss', 'http://www.isotc211.org/2005/gss')
ET.register_namespace('gsr', 'http://www.isotc211.org/2005/gsr')
ET.register_namespace('gfc', 'http://www.isotc211.org/2005/gfc')
ET.register_namespace('gmx', 'http://www.isotc211.org/2005/gmx')
ET.register_namespace('gmi', 'http://www.isotc211.org/2005/gmi')
ET.register_namespace('gml', 'http://www.opengis.net/gml')

#Create a dictionary of metadata for each layer
metadict = {}
reader = csv.reader(open('metadata.csv', 'r'))
for rows in reader:
    filename = rows[0]
    identifier = rows[1]
    title = rows[2]
    description = rows[3]
    creatorIndividual = rows[4].split('|')
    creatorOrganization = rows[5].split('|')
    publisher = rows[6].split('|')
    publicationDate = rows[7]
    isoTopic = rows[8].split('|')
    theme = rows[9].split('|')
    place = rows[10].split('|')
    temporal = rows[11].split('|')
    dataType = rows[12]
    fileFormat = rows[13]
    language = rows[14].split('|')
    uuid = rows[15]
    collectionTitle = rows[16]
    collectionId = rows[17]
    access = rows[18]
    usage = rows[19]
    dataSource = rows[20]
    dataSourceURL = rows[21]
    dataSourceType = rows[22]
    west = rows[23]
    east = rows[24]
    north = rows[25]
    south = rows[26]
    projection = rows[27].split(':')  
    metadict[filename] = identifier, title, description, creatorIndividual, creatorOrganization, publisher, publicationDate, isoTopic, theme, place, temporal, dataType, fileFormat, language, uuid, collectionTitle, collectionId, access, usage, dataSource, dataSourceURL, dataSourceType, west, east, north, south, projection

template = 'iso.xml'
#copy template to new file named for each layer
def applyTemplate(f):
    tree = ET.parse(template)
    root = tree.getroot()
    if f.endswith('.shp') or f.endswith('.tif'):
        new_file = f[:-4] + '.xml'
    else:
        new_file = f[:-8] + '.xml'
    tree.write(new_file) 

def addMetadata(filePath):
    for k, v in metadict.items():
        if f.split('.')[0] == k.split('.')[0]:
           print (f)
           metadataID.text = 'edu.stanford.purl:' + v[0]
           metadataDate.text = strftime("%Y-%m-%d", gmtime())
           identifier.text = 'https://purl.stanford.edu/' + v[0]
           resTitle.text = v[1]
           abstract.text = v[2]
           if v[3][0] == '':
               citation.remove(person)
           else:    
               creatorInd.text = v[3][0]
           for index, ind in enumerate(v[3][0:]):
               if index > 0:
                   citation.insert(6, ET.Element('gmd:citedResponsibleParty'))
                   crp = ET.SubElement(citation[6],'gmd:CI_ResponsibleParty')                   
                   indName = ET.SubElement(crp, 'gmd:individualName')
                   cs = ET.SubElement(crp[0],'gco:CharacterString')
                   cs.text = ind
                   role = ET.SubElement(crp, 'gmd:role')
                   roleCd = ET.SubElement(role, 'gmd:CI_RoleCode')
                   roleCd.text = 'originator'
                   roleCd.set('codeList', 'http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode')
                   roleCd.set('codeListValue','originator')
                   roleCd.set ('codeSpace', 'ISOTC211/19115')               
               
           if v[4][0] == '':
               citation.remove(organisation)             
           else:
               creatorOrg.text = v[4][0]
           for index, org in enumerate(v[4][0:]):
               if index > 0:
                   citation.insert(3, ET.Element('gmd:citedResponsibleParty'))
                   crp = ET.SubElement(citation[3],'gmd:CI_ResponsibleParty')                   
                   orgName = ET.SubElement(crp, 'gmd:organisationName')
                   cs = ET.SubElement(crp[0],'gco:CharacterString')
                   cs.text = org
                   role = ET.SubElement(crp, 'gmd:role')
                   roleCd = ET.SubElement(role, 'gmd:CI_RoleCode')
                   roleCd.text = 'originator'
                   roleCd.set('codeList', 'http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode')
                   roleCd.set('codeListValue','originator')
                   roleCd.set ('codeSpace', 'ISOTC211/19115')
                   
           publisher.text = v[5][0]
           
           if len(pubDate) == 4:
               pubDate.text = v[6] + '-01-01'
               
           topicCategory.text = v[7][0]
           for index, topicCat in enumerate(v[7][0:]):
               if index > 0:
                   DataIdentification.insert(-2, ET.Element('gmd:topicCategory'))
                   tpc = ET.SubElement(DataIdentification[-3],'gmd:MD_TopicCategoryCode')
                   tpc.text = topicCat           
           
           themeKey.text = v[8][0]
           for index, tKey in enumerate(v[8][0:]):
               if index > 0:
                  cs = ET.SubElement(themeKeys[index],'gco:CharacterString')
                  themeKeys.insert(index, ET.Element('gmd:keyword'))
                  themeKeys[index].insert(0, cs)
                  themeKeys[index][0].text = tKey           
           
           placeKey.text = v[9][0]
           for index, pKey in enumerate(v[9][0:]):
               if index > 0:
                  cs = ET.SubElement(placeKeys[index],'gco:CharacterString')
                  placeKeys.insert(index, ET.Element('gmd:keyword'))
                  placeKeys[index].insert(0, cs)
                  placeKeys[index][0].text = pKey           
           
           if len(v[10]) == 1:
               extent.remove(dateRange)
               if len(v[10][0]) == 4:
                   timeInstant.text = v[10][0] + '-01-01T00:00:00'
               if 4 < len(v[10][0]) < 8:
                   temporalEx = datetime.strptime(v[10][0], '%m/%Y')
                   timeInstant.text = datetime.strftime(temporalEx,'%Y-%m-' + '-01T00:00:00')
               if len(v[10][0]) > 7:
                   temporalEx = datetime.strptime(v[10][0], '%m/%d/%Y')
                   timeInstant.text = datetime.strftime(temporalEx,'%Y-%m-%d' + 'T00:00:00')
           if len(v[10]) == 2:
               extent.remove(singleDate)
               if len(v[10][0]) == 4:
                   timePeriodBegin.text = v[10][0] + '-01-01T00:00:00'
                   timePeriodEnd.text = v[10][1] + '-01-01T00:00:00'
               if 4 < len(v[10][0]) < 8:
                   temporalEx = datetime.strptime(v[10][0], '%m/%Y')
                   timePeriodBegin.text = datetime.strftime(temporalEx,'%Y-%m-' + '01T00:00:00')
                   temporalEx = datetime.strptime(v[10][1], '%m/%Y')
                   timePeriodEnd.text = datetime.strftime(temporalEx,'%Y-%m-' + '01T00:00:00')
               if len(v[10][0]) > 7:
                   temporalEx = datetime.strptime(v[10][0], '%m/%d/%Y')
                   timePeriodBegin.text = datetime.strftime(temporalEx,'%Y-%m-%d' + 'T00:00:00')
                   temporalEx = datetime.strptime(v[10][1], '%m/%d/%Y')
                   timePeriodEnd.text = datetime.strftime(temporalEx,'%Y-%m-%d' + 'T00:00:00')

           if v[11] == 'Line data':
               resourceType.text = 'curve'
               resourceType.set('codeListValue', 'curve')
           if v[11] == 'Point data':
               resourceType.text = 'point'
               resourceType.set('codeListValue', 'point')
           if v[11] == 'Polygon data':
               resourceType.text = 'surface'
               resourceType.set('codeListValue', 'surface')
           if v[11] == 'Raster data':
               root.remove(spatialRepresentation)
               
           dataFormat.text = v[12]
           dataLanguage.text = v[13][0]
           dataLanguage.set('codeListValue', v[13][0])
           
           for index, lang in enumerate(v[13][0:]):
               if index > 0:
                  DataIdentification.insert(7, ET.Element('gmd:language'))
                  dl = ET.SubElement(DataIdentification[7],'gmd:LanguageCode')
                  dl.text = lang
                  dl.set('codeList','http://www.loc.gov/standards/iso639-2/php/code_list.php')
                  dl.set('codeListValue',lang)
                  dl.set('codeSpace','ISO639-2')
                  
           if v[14] == '':
               root.remove(contentInfo)
           else:
               fcID.text = v[14]
               
           collTitle.text = v[15]
           collID.text = v[16]
           
           if v[17] == 'public':
               constraints.remove(constraints[0])           
           useConstraints.text = v[18]
           westBL.text = v[22]
           eastBL.text = v[23]
           northBL.text = v[24]
           southBL.text = v[25]
           srsCodeSpace.text = v[26][0]
           srsCode.text = v[26][1]
           if v[19] == '':
               root.remove(dataQuality)
           else:    
               sourceTitle.text = v[19]
               sourceID.text = v[20]
               sourceType.text = v[21]
           distName.text = k
           distURL.text = 'https://purl.stanford.edu/' + v[0]

                   
 
#Walk through the directory and locate data files. Apply the ISO template.          
for dirName, subDirs, fileNames in os.walk('.'):
    for f in fileNames:
        if f.endswith('.geojson') or f.endswith('.shp') or f.endswith('.tif'):
            f = os.path.join(dirName, f)
            applyTemplate(f)

#Walk through the directory and locate ISO metadata files. Find elements by XPath.
for dirName, subDirs, fileNames in os.walk('.'):
    for f in fileNames:
        baseName = f.split('.')
        if f == baseName[0] + '.xml':
            filePath = os.path.join(dirName, f)
            tree = ET.parse(filePath)
            root = tree.getroot()
            identificationInfo = root.find('gmd:identificationInfo', namespaces)
            DataIdentification = identificationInfo.find('gmd:MD_DataIdentification', namespaces)
            metadataID = root.find('gmd:fileIdentifier/gco:CharacterString', namespaces)
            metadataDate = root.find('gmd:dateStamp/gco:Date', namespaces)
            citation = DataIdentification.find('gmd:citation/gmd:CI_Citation', namespaces)
            person = citation.find('gmd:citedResponsibleParty[1]', namespaces)
            organisation = citation.find('gmd:citedResponsibleParty[2]', namespaces)
            pub = citation.find('gmd:citedResponsibleParty[3]', namespaces)
            citedResponsibleParty = citation.find('gmd:citedResponsibleParty', namespaces)
            identifier = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString', namespaces)
            resTitle = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString', namespaces)
            abstract = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString', namespaces)
            creatorInd = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:citation[1]/gmd:CI_Citation[1]/gmd:citedResponsibleParty[1]/gmd:CI_ResponsibleParty[1]/gmd:individualName[1]/gco:CharacterString[1]', namespaces)   
            creatorOrg = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty[2]/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString', namespaces)
            publisher = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty[3]/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString', namespaces)
            pubDate = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date', namespaces)
            topicCategory = root.find('gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode', namespaces)
            themeKey = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:descriptiveKeywords[2]/gmd:MD_Keywords[1]/gmd:keyword[1]/gco:CharacterString', namespaces)
            themeKeys = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:descriptiveKeywords[2]/gmd:MD_Keywords[1]', namespaces)
            placeKey = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:descriptiveKeywords[1]/gmd:MD_Keywords[1]/gmd:keyword[1]/gco:CharacterString[1]', namespaces)
            placeKeys = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:descriptiveKeywords[1]/gmd:MD_Keywords[1]', namespaces)
            extent = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]', namespaces)
            singleDate = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:temporalElement[1]', namespaces)
            dateRange = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:temporalElement[2]', namespaces)
            timeInstant = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:temporalElement[1]/gmd:EX_TemporalExtent[1]/gmd:extent[1]/gml:TimeInstant[1]/gml:timePosition[1]', namespaces)
            timePeriodBegin = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:temporalElement[2]/gmd:EX_TemporalExtent[1]/extent[1]/gml:TimePeriod[1]/gml:beginPosition[1]', namespaces)
            timePeriodEnd = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:temporalElement[2]/gmd:EX_TemporalExtent[1]/extent[1]/gml:TimePeriod[1]/gml:endPosition[1]', namespaces)
            resourceType = root.find('gmd:spatialRepresentationInfo/gmd:MD_VectorSpatialRepresentation/gmd:geometricObjects/gmd:MD_GeometricObjects/gmd:geometricObjectType/gmd:MD_GeometricObjectTypeCode', namespaces)
            dataFormat = root.find('gmd:distributionInfo[1]/gmd:MD_Distribution[1]/gmd:distributionFormat[1]/gmd:MD_Format[1]/gmd:name[1]/gco:CharacterString[1]', namespaces)
            dataLanguage = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:language[1]/gmd:LanguageCode', namespaces)
            contentInfo =  root.find('gmd:contentInfo[1]', namespaces)
            fcID = root.find('gmd:contentInfo[1]/gmd:MD_FeatureCatalogueDescription[1]/gmd:featureCatalogueCitation[1]/gmd:CI_Citation[1]/gmd:identifier[1]/gmd:MD_Identifier[1]/gmd:code[1]/gco:CharacterString[1]', namespaces)
            collTitle = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:aggregationInfo[1]/gmd:MD_AggregateInformation[1]/gmd:aggregateDataSetName[1]/gmd:CI_Citation[1]/gmd:title[1]/gco:CharacterString[1]', namespaces)
            collID = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:aggregationInfo[1]/gmd:MD_AggregateInformation[1]/gmd:aggregateDataSetName[1]/gmd:CI_Citation[1]/gmd:identifier[1]/gmd:MD_Identifier[1]/gmd:code[1]/gco:CharacterString[1]', namespaces)
            constraints = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/resourceConstraints[1]/MD_LegalConstraints[1]', namespaces)
            accessConstraints = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/resourceConstraints[1]/MD_LegalConstraints[1]/accessConstraints[1]', namespaces)
            useConstraints = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/resourceConstraints[1]/MD_LegalConstraints[1]/gmd:otherConstraints[1]/gco:CharacterString[1]', namespaces)
            dataQuality = root.find('dataQualityInfo[1]', namespaces)
            sourceTitle = root.find('dataQualityInfo[1]/DQ_DataQuality[1]/lineage[1]/LI_Lineage[1]/source[1]/LI_Source[1]/sourceCitation[1]/CI_Citation[1]/title[1]/gco:CharacterString[1]', namespaces)
            sourceID = root.find('dataQualityInfo[1]/DQ_DataQuality[1]/lineage[1]/LI_Lineage[1]/source[1]/LI_Source[1]/sourceCitation[1]/CI_Citation[1]/identifier[1]/MD_Identifier[1]/code[1]/gco:CharacterString[1]', namespaces)
            sourceType = root.find('dataQualityInfo[1]/DQ_DataQuality[1]/lineage[1]/LI_Lineage[1]/source[1]/LI_Source[1]/description[1]/gco:CharacterString[1]', namespaces)
            spatialRepresentation = root.find('gmd:spatialRepresentationInfo[1]', namespaces)
            westBL = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:geographicElement[1]/gmd:EX_GeographicBoundingBox[1]/gmd:westBoundLongitude[1]/gco:Decimal[1]', namespaces)
            eastBL = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:geographicElement[1]/gmd:EX_GeographicBoundingBox[1]/gmd:eastBoundLongitude[1]/gco:Decimal[1]', namespaces)
            northBL = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:geographicElement[1]/gmd:EX_GeographicBoundingBox[1]/gmd:northBoundLatitude[1]/gco:Decimal[1]', namespaces)
            southBL = root.find('gmd:identificationInfo[1]/gmd:MD_DataIdentification[1]/gmd:extent[1]/gmd:EX_Extent[1]/gmd:geographicElement[1]/gmd:EX_GeographicBoundingBox[1]/gmd:southBoundLatitude[1]/gco:Decimal[1]', namespaces)
            srsCodeSpace = root.find('gmd:referenceSystemInfo[1]/gmd:MD_ReferenceSystem[1]/gmd:referenceSystemIdentifier[1]/gmd:RS_Identifier[1]/gmd:codeSpace[1]/gco:CharacterString[1]', namespaces)
            distName = root.find('gmd:distributionInfo[1]/gmd:MD_Distribution[1]/transferOptions[1]/MD_DigitalTransferOptions[1]/onLine[1]/CI_OnlineResource[1]/name[1]/gco:CharacterString[1]', namespaces)
            distURL = root.find('gmd:distributionInfo[1]/gmd:MD_Distribution[1]/transferOptions[1]/MD_DigitalTransferOptions[1]/onLine[1]/CI_OnlineResource[1]/linkage[1]/URL[1]', namespaces)
            srsCode = root.find('gmd:referenceSystemInfo[1]/gmd:MD_ReferenceSystem[1]/gmd:referenceSystemIdentifier[1]/gmd:RS_Identifier[1]/gmd:code[1]/gco:CharacterString[1]', namespaces)
            addMetadata(filePath)
            tree.write(filePath)

            
