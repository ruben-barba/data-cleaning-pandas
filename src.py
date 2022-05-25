
import pandas as pd
import numpy as np
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
import time
from tqdm import tqdm
from dateutil.parser import parse
import regex as re

def date_clean(df,column):
    """
    df : DataFrame
    column : nombre de la columna a convertir en tipo datetime (string)
    
    La funcion date_clean acepta como parametro un DataFrame y el nombre de la columna que quieres limpiar.
    Devuelve una Serie con todos los valores de la columna seleccionada convertidos a tipo Datetime.
    Los que no ha podido convertir, los pone nulos.
    """
    print(f"Convirtiendo la columna *{df[column].name}* a tipo Datetime")
    
    df[column] = [re.sub(r"[\s/_-]",".",str(i)) for i in df[column]]
    
    for i in tqdm(df.index):
    
        try:
            df.loc[i,column] = datetime.strptime("".join(re.findall(r"(?:1[5-9]\d{2}|20[0-2][0-9]|2022)[-/.](?:0[1-9]|1[012])[-/.](?:0[1-9]|[12][0-9]|3[01])",str(df[column][i]))),"%Y.%m.%d").date()
        except ValueError:      
            pass

            try: 
                df.loc[i,column] = datetime.strptime("".join(re.findall(r"(?:1[5-9]\d{2}|20[0-2][0-9]|2022)[-/.](?:0[1-9]|1[012])",str(df[column][i]))),"%Y.%m").date()
            except ValueError:
                pass

                try: 
                    df.loc[i,column] = datetime.strptime("".join(re.findall(r"(?:1[5-9]\d{2}|20[0-2][0-9]|2022)",str(df[column][i]))),"%Y").year
                except ValueError:
                    df.loc[i,column] = np.nan
    
    return


def new_year(column):
    """
    column : nombre columna de tipo datetime (string)
    
    La funcion new_year extrae el año de la columna tipo Datetime que le pases como parametro.
    Devuelve una lista con años tipo Int.
    Los años que no ha podido extraer serán nulos.
    """
    print(f"Extrayendo los años de la columna *{column.name}*") 
        
    new_year_2 = []
    
    for i in tqdm(column):
        
        if isinstance(i,datetime):
            new_year_2.append(i.year)
            
        elif isinstance(i,int):
            new_year_2.append(i)
            
        else:
            new_year_2.append(np.nan)
            
    return new_year_2 


def year_clean(df,year_1,year_2,compr_column):
    """
    df : DataFrame
    year_1 : nombre de la columna 1 (string)
    year_2 : nombre de la columna 2 (string)
    compr_column : nombre de la columna de comprobacion (string)
    
    La funcion coge como parametros el dataframe, dos columnas con años que se quieran comparar (year_1 = columna antigua, 
    year_2 = columna nueva), y una tercera columna que sirva como criterio de comprobacion.
    Sustituye los valores en la columna year_2 si los de year_1 hacen match con la columna compr_column.
    """
    
    print(f"Comparando las columnas *{df[year_1].name}* y *{df[year_2].name}* con la columna *{df[compr_column].name}*")
    
    count_1 = 0
    count_2 = 0
    
    for i in tqdm(df.index):
        
        if df[year_1][i] != df[year_2][i]:
            count_1 += 1
            
            if str(df[year_1][i])[0:4]=="".join(re.findall(r"(?:1[5-9]\d{2}|20[0-2][0-9]|2022)",str(df[compr_column][i]))):
                df.loc[i,year_2] = df[year_1][i]
                count_2 += 1
                continue

            else:
                continue   
                
    return f"De {count_1} valores que diferían, {count_2} han sido substituidos"

def fatal_clean(df, col):
    
    '''
    df : DataFrame
    col : nombre de la columna a limpiar (string)
    
    La función fatal_clean recibe como parametro un DataFrame y el nombre de la columna donde se encuentran los datos binarios fatal
    El objetivo es que en la columna solo haya los siguientes tipos de valores-> Y(Yes), N(No), NaN(sin información)
    1. Todos los datos no nulos se pasan a mayúsculas
    2. Se eliminan espacios anteriores o posteriores en todos los datos no nulos
    3. Todos los datos nulos se definen como NaN
    4. Todo lo que no sea 'Y' o 'N' se define como NaN
    
    '''
    
    print(f"Limpiando la columna *{df[col].name}*")
    
    df[col] = df[col].str.upper().str.strip()

    for ind in tqdm(df.index):
        
        if df[col][ind] != 'Y' and df[col][ind] != 'N':
            df.loc[ind,col] = np.nan
               
    return

def fatal_catcher(df, col1, col2):
    
    '''
    df : DataFrame
    col1 : nombre de la columna 1 a limpiar (string)
    col2 : nombre de la columna 2 a limpiar (string)
    
    La función fatal_catcher recibe como parametro un DataFrame, el nombre de la columna col1 donde se encuentran los datos
    binarios de fatal, y la columna col2 donde se encuentran las descripciones de las lesiones producidas
    El objetivo es encontrar descripciones de lesiones con la palabra fatal incluida
    Se pasan todas las strings a uppercase, se verifica registro pro registro que tratamos con un string para que no nos de
    error al aplicarle un filtro regex, buscamos la palabra fatal. Al encontrar un registro con la palabra fatal,
    verificamos si constaba en la columna fatal como Y(Yes), sino es asi, lo actualizamos a Y(Yes).
    '''
    print(f"Comparando las columnas *{df[col1].name}* y *{df[col2].name}*")
    
    fc=0
    
    df[col2]=df[col2].str.upper()
   
    
    for ind in tqdm(df.index):
       
        if isinstance(df[col2][ind], str):
            
            if re.search(r'\w*FATAL', df[col2][ind]) and df[col1][ind]!='Y':
                
                df.loc[ind,col1]='Y'
                
                fc=fc+1

    return f'{fc} registros han sido actualizados como fatales a partir de la descripción de las heridas'



def country_clean(df, col):

    '''
    df : DataFrame
    col : nombre de la columna a limpiar (string)
    
    La función country_clean normaliza lista paises, las ubicaciones desconocidas se pasan a nulos
    
    '''
    
    print(f"Limpiando la columna *{df[col].name}*")

    country_names=[
         'USA',
         'AFGHANISTAN',
         'ALBANIA',
         'ALGERIA',
         'AMERICAN SAMOA',
         'ANDORRA',
         'ANGOLA',
         'ANGUILLA',
         'ANTARCTICA',
         'ANTIGUA AND BARBUDA',
         'ARGENTINA',
         'ARMENIA',
         'ARUBA',
         'AUSTRALIA',
         'AUSTRIA',
         'AZERBAIJAN',
         'BAHAMAS',
         'BAHREIN',
         'BANGLADESH',
         'BARBADOS',
         'BELARUS',
         'BELGIUM',
         'BELIZE',
         'BENIN',
         'BERMUDA',
         'BHUTAN',
         'BOLIVIA',
         'BOSNIA AND HERZEGOWINA',
         'BOTSWANA',
         'BOUVET ISLAND',
         'BRAZIL',
         'BRUNEI DARUSSALAM',
         'BULGARIA',
         'BURKINA FASO',
         'BURUNDI',
         'CAMBODIA',
         'CAMEROON',
         'CANADA',
         'CAPE VERDE',
         'CAYMAN ISLANDS',
         'CENTRAL AFRICAN REP',
         'CHAD',
         'CHILE',
         'CHINA',
         'CHRISTMAS ISLAND',
         'COCOS ISLANDS',
         'COLOMBIA',
         'COMOROS',
         'CONGO',
         'COOK ISLANDS',
         'COSTA RICA',
         'COTE D`IVOIRE',
         'CROATIA',
         'CUBA',
         'CYPRUS',
         'CZECH REPUBLIC',
         'DENMARK',
         'DJIBOUTI',
         'DOMINICA',
         'DOMINICAN REPUBLIC',
         'EAST TIMOR',
         'ECUADOR',
         'EGYPT',
         'EL SALVADOR',
         'EQUATORIAL GUINEA',
         'ERITREA',
         'ESTONIA',
         'ETHIOPIA',
         'FALKLAND ISLANDS (MALVINAS)',
         'FAROE ISLANDS',
         'FIJI',
         'FINLAND',
         'FRANCE',
         'FRENCH GUIANA',
         'FRENCH POLYNESIA',
         'FRENCH S. TERRITORIES',
         'GABON',
         'GAMBIA',
         'GEORGIA',
         'GERMANY',
         'GHANA',
         'GIBRALTAR',
         'GREECE',
         'GREENLAND',
         'GRENADA',
         'GUADELOUPE',
         'GUAM',
         'GUATEMALA',
         'GUINEA',
         'GUINEA-BISSAU',
         'GUYANA',
         'HAITI',
         'HONDURAS',
         'HONG KONG',
         'HUNGARY',
         'ICELAND',
         'INDIA',
         'INDONESIA',
         'IRAN',
         'IRAQ',
         'IRELAND',
         'ISRAEL',
         'ITALY',
         'JAMAICA',
         'JAPAN',
         'JORDAN',
         'KAZAKHSTAN',
         'KENYA',
         'KIRIBATI',
         'NORTH KOREA',
         'SOUTH KOREA',
         'KUWAIT',
         'KYRGYZSTAN',
         'LAOS',
         'LATVIA',
         'LEBANON',
         'LESOTHO',
         'LIBERIA',
         'LIBYA',
         'LIECHTENSTEIN',
         'LITHUANIA',
         'LUXEMBOURG',
         'MACAU',
         'MACEDONIA',
         'MADAGASCAR',
         'MALAWI',
         'MALAYSIA',
         'MALDIVES',
         'MALI',
         'MALTA',
         'MARSHALL ISLANDS',
         'MARTINIQUE',
         'MAURITANIA',
         'MAURITIUS',
         'MAYOTTE',
         'MEXICO',
         'MICRONESIA',
         'MOLDOVA',
         'MONACO',
         'MONGOLIA',
         'MONTENEGRO',
         'MONTSERRAT',
         'MOROCCO',
         'MOZAMBIQUE',
         'MYANMAR',
         'NAMIBIA',
         'NAURU',
         'NEPAL',
         'NETHERLANDS',
         'NETHERLANDS ANTILLES',
         'NEW CALEDONIA',
         'NEW ZEALAND',
         'NICARAGUA',
         'NIGER',
         'NIGERIA',
         'NIUE',
         'NORFOLK ISLAND',
         'NORTHERN MARIANA ISLANDS',
         'NORWAY',
         'OMAN',
         'PAKISTAN',
         'PALAU',
         'PANAMA',
         'PAPUA NEW GUINEA',
         'PARAGUAY',
         'PERU',
         'PHILIPPINES',
         'PITCAIRN',
         'POLAND',
         'PORTUGAL',
         'PUERTO RICO',
         'QATAR',
         'REUNION',
         'ROMANIA',
         'RUSSIA',
         'RWANDA',
         'SAINT KITTS AND NEVIS',
         'SAINT LUCIA',
         'ST VINCENT/GRENADINES',
         'SAMOA',
         'SAN MARINO',
         'SAO TOME',
         'SAUDI ARABIA',
         'SENEGAL',
         'SEYCHELLES',
         'SIERRA LEONE',
         'SINGAPORE',
         'SLOVAKIA',
         'SLOVENIA',
         'SOLOMON ISLANDS',
         'SOMALIA',
         'SOUTH AFRICA',
         'SPAIN',
         'SRI LANKA',
         'ST. HELENA',
         'ST.PIERRE',
         'SUDAN',
         'SURINAME',
         'SWAZILAND',
         'SWEDEN',
         'SWITZERLAND',
         'SYRIA',
         'TAIWAN',
         'TAJIKISTAN',
         'TANZANIA',
         'THAILAND',
         'TOGO',
         'TOKELAU',
         'TONGA',
         'TRINIDAD & TOBAGO',
         'TUNISIA',
         'TURKEY',
         'TURKMENISTAN',
         'TUVALU',
         'UGANDA',
         'UKRAINE',
         'UNITED ARAB EMIRATES (UAE)',
         'UNITED KINGDOM',
         'URUGUAY',
         'UZBEKISTAN',
         'VANUATU',
         'VATICAN CITY STATE',
         'VENEZUELA',
         'VIETNAM',
         'VIRGIN ISLANDS (BRITISH)',
         'VIRGIN ISLANDS (U.S.)',
         'WESTERN SAHARA',
         'YEMEN',
         'YUGOSLAVIA',
         'ZAIRE',
         'ZAMBIA',
         'ZIMBABWE',
         'ST HELENA, BRITISH OVERSEAS TERRITORY',
         'TURKS & CAICOS, BRITISH OVERSEAS TERRITORY',
         'PALESTINIAN TERRITORIES',
         'DIEGO GARCIA, BRITISH OVERSEAS TERRITORY',
         'FALKLAND ISLANDS, BRITISH OVERSEAS TERRITORY',
         'REUNION ISLAND, FRENCH OVERSEAS TERRITORY'
        ]

    df[col] = df[col].str.upper().str.strip()

    corrections=[
            ('ENGLAND','UNITED KINGDOM'),
            ('SCOTLAND','UNITED KINGDOM'),
            ('COLUMBIA','CANADA'),
            ('OKINAWA','JAPAN'),
            ('TURKS & CAICOS', 'TURKS & CAICOS, BRITISH OVERSEAS TERRITORY'),
            ('AZORES','PORTUGAL'),
            ('UNITED ARAB EMIRATES','UNITED ARAB EMIRATES (UAE)'),
            ('NEW BRITAIN','PAPUA NEW GUINEA'),
            ('DIEGO GARCIA','DIEGO GARCIA, BRITISH OVERSEAS TERRITORY'),
            ('MALDIVE ISLANDS','FALKLAND ISLANDS, BRITISH OVERSEAS TERRITORY'),
            ('FALKLAND ISLANDS','FALKLAND ISLANDS, BRITISH OVERSEAS TERRITORY'),
            ('NEW GUINEA','PAPUA NEW GUINEA'),
            ('CRETE','GREECE'),
            ('CEYLON','SRI LANKA'),
            ('CEYLON (SRI LANKA)','SRI LANKA'),
            ('BURMA','MYANMAR'),
            ('TOBAGO','TRINIDAD & TOBAGO'),
            ('BRITISH NEW GUINEA','PAPUA NEW GUINEA'),
            ('REUNION ISLAND','REUNION ISLAND, FRENCH OVERSEAS TERRITORY'),
            ('JAVA','INDONESIA'),
            ('ST. MARTIN','ST.MARTIN, FRENCH OVERSEAS TERRITORY'),
            ('ST. MAARTIN','ST.MARTIN, FRENCH OVERSEAS TERRITORY')
        ]

    count_1 = 0
    count_2 = 0
    
    for x,y in corrections:
    
        for ind in df.index:

            if df[col][ind] == x: 
                
                df.loc[ind,col] = y
                count_1 += 1

    for ind in tqdm(df.index):

        if df[col][ind] not in country_names: 
            
            df.loc[ind,col] = np.nan
            count_2 += 1
            
    return f"Se han corregido {count_1} registros. Se han puesto como nulos {count_2} registros"

def species_clean(df, col):
    """
    df : DataFrame
    col1 : nombre de la columna 1 a limpiar (string)
    
    La funcion species_clean limpia la columna que le pases como parametro tratando de identificar el contenido de la iteracion 
    dentro de una libreria de tiburones. Si no logra identificarlo dentro de la biblioteca, lo pone como nulo.
    
    """
    
    #ponemos las species en mayusculas y quitamos espacios anteriores y posteriores al string
    df[col] = df[col].str.upper().str.strip()
    
    #definimos biblioteca de especies
    sharks_biblio=['BLACKNOSE SHARK',
     'SILVERTIP SHARK',
     'BIGNOSE SHARK',
     'GRACEFUL SHARK',
     'GREY REEF SHARK',
     'PIGEYE SHARK',
     'BORNEO SHARK',
     'COPPER SHARK',
     'SPINNER SHARK',
     'NERVOUS SHARK',
     "COATES'S SHARK",
     'WHITECHEEK SHARK',
     'SILKY SHARK',
     'CREEK WHALER',
     'GALAPAGOS SHARK',
     'PONDICHERRY SHARK',
     "HUMAN'S WHALER SHARK",
     'FINETOOTH SHARK',
     'SMOOTHTOOTH BLACKTIP SHARK',
     'BULL SHARK',
     'BLACKTIP SHARK',
     'OCEANIC WHITETIP SHARK',
     'HARDNOSE SHARK',
     'BLACKTIP REEF SHARK',
     'DUSKY SHARK',
     'CARIBBEAN REEF SHARK',
     'SANDBAR SHARK',
     'SMALLTAIL SHARK',
     'BLACKSPOT SHARK',
     'NIGHT SHARK',
     'SPOT-TAIL SHARK',
     'AUSTRALIAN BLACKTIP SHARK',
     'INDONESIAN WHALER SHARK',
     'TIGER SHARK',
     'BORNEO RIVER SHARK',
     'GANGES SHARK',
     'NORTHERN RIVER SHARK',
     'SPEARTOOTH SHARK',
     'IRRAWADDY RIVER SHARK',
     'DAGGERNOSE SHARK',
     'BROADFIN SHARK',
     'BORNEO BROADFIN SHARK',
     'SLITEYE SHARK',
     'WHITENOSE SHARK',
     'SICKLEFIN LEMON SHARK',
     'LEMON SHARK',
     'BLUE SHARK',
     'MILK SHARK',
     'BRAZILIAN SHARPNOSE SHARK',
     'PACIFIC SHARPNOSE SHARK',
     'GREY SHARPNOSE SHARK',
     'CARIBBEAN SHARPNOSE SHARK',
     'AUSTRALIAN SHARPNOSE SHARK',
     'ATLANTIC SHARPNOSE SHARK',
     'SPADENOSE SHARK',
     'PACIFIC SPADENOSE SHARK',
     'HOOKTOOTH SHARK',
     'AUSTRALIAN WEASEL SHARK',
     'SICKLEFIN WEASEL SHARK',
     'SNAGGLETOOTH SHARK',
     'WHITETIP WEASEL SHARK',
     'ATLANTIC WEASEL SHARK',
     'SLENDER WEASEL SHARK',
     'STRAIGHT-TOOTH WEASEL SHARK',
     'BARBELED HOUNDSHARK',
     'HARLEQUIN CATSHARK',
     'CUBAN RIBBONTAIL CATSHARK',
     'PYGMY RIBBONTAIL CATSHARK',
     'AFRICAN RIBBONTAIL CATSHARK',
     'GRACEFUL CATSHARK',
     'MAGNIFICENT CATSHARK',
     'DWARF FALSE CATSHARK',
     'FALSE CATSHARK',
     'WHITE-BODIED CATSHARK',
     'ROUGHSKIN CATSHARK',
     'WHITE GHOST CATSHARK',
     'PINOCCHIO CATSHARK',
     'BROWN CATSHARK',
     'BIGHEAD CATSHARK',
     'HOARY CATSHARK',
     'FLACCID CATSHARK',
     "FEDOROV'S CATSHARK",
     "GARRICK'S CATSHARK",
     'HUMPBACK CATSHARK',
     'LONGFIN CATSHARK',
     'SMALLBELLY CATSHARK',
     'SHORTNOSE DEMON CATSHARK',
     'BROADNOSE CATSHARK',
     'JAPANESE CATSHARK',
     'LONGNOSE CATSHARK',
     'ICELAND CATSHARK',
     'LONGHEAD CATSHARK',
     'FLATHEAD CATSHARK',
     'BROADMOUTH CATSHARK',
     'GHOST CATSHARK',
     'BLACK ROUGHSCALE CATSHARK',
     'SMALLEYE CATSHARK',
     'SMALLDORSAL CATSHARK',
     'MILK-EYE CATSHARK',
     'LARGENOSE CATSHARK',
     'SMALLFIN CATSHARK',
     'FAT CATSHARK',
     'SPATULASNOUT CATSHARK',
     'DEEP-WATER CATSHARK',
     'BROADGILL CATSHARK',
     'SALDANHA CATSHARK',
     'PALE CATSHARK',
     'SOUTH CHINA CATSHARK',
     'SPONGEHEAD CATSHARK',
     'PANAMA GHOST CATSHARK',
     'AUSTRALIAN SPOTTED CATSHARK',
     'BLOTCHED CATSHARK',
     'STARRY CATSHARK',
     'WESTERN SPOTTED CATSHARK',
     'PALE SPOTTED CATSHARK',
     'DWARF CATSHARK',
     'ORANGE SPOTTED CATSHARK',
     'VARIEGATED CATSHARK',
     'GULF CATSHARK',
     'BALI CATSHARK',
     'BANDED SAND CATSHARK',
     'AUSTRALIAN MARBLED CATSHARK',
     'CORAL CATSHARK',
     'EASTERN BANDED CATSHARK',
     'NEW CALEDONIA CATSHARK',
     'AUSTRALIAN BLACKSPOTTED CATSHARK',
     'DUSKY CATSHARK',
     'BROADHEAD CATSHARK',
     'NEW ZEALAND CATSHARK',
     'GALAPAGOS CATSHARK',
     'BRISTLY CATSHARK',
     'SPOTLESS CATSHARK',
     'SOMBRE CATSHARK',
     'MUD CATSHARK',
     'WHITEFIN SWELLSHARK',
     "COOK'S SWELLSHARK",
     'RETICULATED SWELLSHARK',
     'FORMOSA SWELLSHARK',
     'AUSTRALIAN RETICULATE SWELLSHARK',
     'DRAUGHTSBOARD SHARK',
     'AUSTRALIAN SWELLSHARK',
     'PAINTED SWELLSHARK',
     'SARAWAK PYGMY SWELLSHARK',
     'FLAGTAIL SWELLSHARK',
     'INDIAN SWELLSHARK',
     'SPECKLED SWELLSHARK',
     "STEVEN'S SWELLSHARK",
     'BALLOON SHARK',
     'BLOTCHY SWELLSHARK',
     'SADDLED SWELLSHARK',
     'SWELLSHARK',
     'NARROWBAR SWELLSHARK',
     'LOLLIPOP CATSHARK',
     'AUSTRALIAN SAWTAIL CATSHARK',
     'NORTHERN SAWTAIL CATSHARK',
     'ANTILLES CATSHARK',
     'ROUGHTAIL CATSHARK',
     'ATLANTIC SAWTAIL CATSHARK',
     'LONGFIN SAWTAIL CATSHARK',
     'GECKO CATSHARK',
     'SLENDER SAWTAIL CATSHARK',
     'LONGNOSE SAWTAIL CATSHARK',
     'BLACKMOUTH CATSHARK',
     'SOUTHERN SAWTAIL CATSHARK',
     'MOUSE CATSHARK',
     'BROADFIN SAWTAIL CATSHARK',
     'PEPPERED CATSHARK',
     'AFRICAN SAWTAIL CATSHARK',
     'PHALLIC CATSHARK',
     'BLACKTIP SAWTAIL CATSHARK',
     'DWARF SAWTAIL CATSHARK',
     "SPRINGER'S SAWTAIL CATSHARK",
     'SPECKLED CATSHARK',
     'BLACKSPOTTED CATSHARK',
     'LINED CATSHARK',
     'INDONESIAN SPECKLED CATSHARK',
     'TIGER CATSHARK',
     'QUAGGA CATSHARK',
     'RUSTY CATSHARK',
     'PUFFADDER SHYSHARK',
     'BROWN SHYSHARK',
     'NATAL SHYSHARK',
     'DARK SHYSHARK',
     'HONEYCOMB IZAK',
     'GRINNING IZAK',
     'CRYING IZAK',
     'WHITE-SPOTTED IZAK',
     'IZAK CATSHARK',
     'WHITE-TIP CATSHARK',
     'WHITE-CLASPER CATSHARK',
     'BEIGE CATSHARK',
     'CAMPECHE CATSHARK',
     'VELVET CATSHARK',
     "MCMILLAN'S CAT SHARK",
     'BLACKGILL CATSHARK',
     'SALAMANDER SHARK',
     'FILETAIL CATSHARK',
     'ONEFIN CATSHARK',
     'PYJAMA SHARK',
     'LEOPARD CATSHARK',
     'NARROWMOUTHED CATSHARK',
     'REDSPOTTED CATSHARK',
     'NARROWTAIL CATSHARK',
     'LIZARD CATSHARK',
     'SLENDER CATSHARK',
     'POLKADOT CATSHARK',
     'BOA CATSHARK',
     'SMALL-SPOTTED CATSHARK',
     'YELLOW-SPOTTED CATSHARK',
     'WEST AFRICAN CATSHARK',
     'COMORO CATSHARK',
     'BROWNSPOTTED CATSHARK',
     'FRECKLED CATSHARK',
     'WHITE-SADDLED CATSHARK',
     'BLOTCHED CATSHARK',
     'CHAIN CATSHARK',
     'NURSEHOUND',
     'IZU CATSHARK',
     'CLOUDY CATSHARK',
     'DWARF CATSHARK',
     'WINGHEAD SHARK',
     'SCALLOPED BONNETHEAD',
     'CAROLINA HAMMERHEAD',
     'SCALLOPED HAMMERHEAD',
     'SCOOPHEAD',
     'GREAT HAMMERHEAD',
     'BONNETHEAD',
     'SMALLEYE HAMMERHEAD',
     'SMOOTH HAMMERHEAD',
     'WHISKERY SHARK',
     'TOPE SHARK',
     'SAILBACK HOUNDSHARK',
     'DEEPWATER SICKLEFIN HOUNDSHARK',
     'OCELLATE TOPESHARK',
     'SICKLEFIN HOUNDSHARK',
     'INDONESIAN HOUNDSHARK',
     'JAPANESE TOPESHARK',
     'WHITEFIN TOPESHARK',
     'BLACKTIP TOPE',
     'LONGNOSE HOUNDSHARK',
     'MANGALORE HOUNDSHARK',
     'BIGEYE HOUNDSHARK',
     'BENGAL SMALLGILL HOUNDSHARK',
     'WHITE-MARGIN FIN HOUNDSHARK',
     'GUMMY SHARK',
     'STARRY SMOOTH-HOUND',
     'GREY SMOOTH-HOUND',
     'DUSKY SMOOTH-HOUND',
     'SHARPTOOTH SMOOTH-HOUND',
     'STRIPED SMOOTH-HOUND',
     'SPOTLESS SMOOTH-HOUND',
     'BROWN SMOOTH-HOUND',
     'SMALLEYE SMOOTH-HOUND',
     'SPOTTED ESTUARY SMOOTH-HOUND',
     'SICKLEFIN SMOOTH-HOUND',
     'STARSPOTTED SMOOTH-HOUND',
     'SPECKLED SMOOTH-HOUND',
     'DWARF SMOOTH-HOUND',
     'ARABIAN SMOOTH-HOUND',
     'COMMON SMOOTH-HOUND',
     'NARROWFIN SMOOTH-HOUND',
     'WHITESPOTTED SMOOTH-HOUND',
     'BLACKSPOTTED SMOOTH-HOUND',
     'AUSTRALIAN GREY SMOOTH-HOUND',
     'NARROWNOSE SMOOTH-HOUND',
     'GULF SMOOTHHOUND',
     'WESTERN SPOTTED GUMMY SHARK',
     'EASTERN SPOTTED GUMMY SHARK',
     'HUMPBACK SMOOTH-HOUND',
     'WHITE-FIN SMOOTH-HOUND',
     'FLAPNOSE HOUNDSHARK',
     'SHARPFIN HOUNDSHARK',
     'SPOTTED HOUNDSHARK',
     'SHARPTOOTH HOUNDSHARK',
     'BANDED HOUNDSHARK',
     'LEOPARD SHARK',
     'BRAMBLE SHARK',
     'PRICKLY SHARK',
     'HORN SHARK',
     'CRESTED BULLHEAD SHARK',
     'JAPANESE BULLHEAD SHARK',
     'MEXICAN HORNSHARK',
     'OMAN BULLHEAD SHARK',
     'PORT JACKSON SHARK',
     'GALAPAGOS BULLHEAD SHARK',
     'WHITESPOTTED BULLHEAD SHARK',
     'ZEBRA BULLHEAD SHARK',
     'SOUTHERN AFRICAN FRILLED SHARK',
     'FRILLED SHARK',
     'SHARPNOSE SEVENGILL SHARK',
     'BLUNTNOSE SIXGILL SHARK',
     'BIGEYED SIXGILL SHARK',
     'BROADNOSE SEVENGILL SHARK',
     'PELAGIC THRESHER',
     'BIGEYE THRESHER',
     'COMMON THRESHER',
     'BASKING SHARK',
     'GREAT WHITE SHARK',
     'SHORTFIN MAKO',
     'LONGFIN MAKO',
     'SALMON SHARK',
     'PORBEAGLE',
     'MEGAMOUTH SHARK',
     'GOBLIN SHARK',
     'SAND TIGER SHARK',
     'SMALLTOOTH SAND TIGER',
     'BIGEYE SAND TIGER',
     'CROCODILE SHARK',
     'BLUEGREY CARPETSHARK',
     'BLIND SHARK',
     'NURSE SHARK',
     'TAWNY NURSE SHARK',
     'SHORT-TAIL NURSE SHARK',
     'ARABIAN CARPETSHARK',
     'BURMESE BAMBOO SHARK',
     'GREY BAMBOO SHARK',
     "HASSELT'S BAMBOO SHARK",
     'SLENDER BAMBOO SHARK',
     'WHITESPOTTED BAMBOO SHARK',
     'BROWNBANDED BAMBOO SHARK',
     'INDONESIAN SPECKLED CARPETSHARK',
     'CENDERWASIH EPAULETTE SHARK',
     'PAPUAN EPAULETTE SHARK',
     'HALMAHERA EPAULETTE SHARK',
     "HENRY'S EPAULETTE SHARK",
     'MILNE BAY EPAULETTE SHARK',
     'EPAULETTE SHARK',
     'HOODED CARPETSHARK',
     'SPECKLED CARPETSHARK',
     'TASSELLED WOBBEGONG',
     'FLORAL BANDED WOBBEGONG',
     'GULF WOBBEGONG',
     'WESTERN WOBBEGONG',
     'JAPANESE WOBBEGONG',
     'INDONESIAN WOBBEGONG',
     'SPOTTED WOBBEGONG',
     'ORNATE WOBBEGONG',
     'DWARF SPOTTED WOBBEGONG',
     'NETWORK WOBBEGONG',
     'NORTHERN WOBBEGONG',
     'COBBLER WOBBEGONG',
     'BARBELTHROAT CARPETSHARK',
     'TAIWAN SADDLED CARPETSHARK',
     'SADDLED CARPETSHARK',
     'COLLARED CARPETSHARK',
     'ELONGATE CARPETSHARK',
     'RUSTY CARPETSHARK',
     'GINGER CARPETSHARK',
     'NECKLACE CARPETSHARK',
     'WHALE SHARK',
     'ZEBRA SHARK',
     'SIXGILL SAWSHARK',
     'LONGNOSE SAWSHARK',
     'TROPICAL SAWSHARK',
     'JAPANESE SAWSHARK',
     "LANA'S SAWSHARK",
     'AFRICAN DWARF SAWSHARK',
     'SHORTNOSE SAWSHARK',
     'BAHAMAS SAWSHARK',
     'DWARF GULPER SHARK',
     'GULPER SHARK',
     'DUMB GULPER SHARK',
     'BLACKFIN GULPER SHARK',
     'LOWFIN GULPER SHARK',
     'SMALLFIN GULPER SHARK',
     'SEYCHELLES GULPER SHARK',
     'LEAFSCALE GULPER SHARK',
     'MOSAIC GULPER SHARK',
     'WESTERN GULPER SHARK',
     'SOUTHERN DOGFISH',
     'BIRDBEAK DOGFISH',
     'ROUGH LONGNOSE DOGFISH',
     'ARROWHEAD DOGFISH',
     'LONGSNOUT DOGFISH',
     'KITEFIN SHARK',
     'TAILLIGHT SHARK',
     'PYGMY SHARK',
     'LONGNOSE PYGMY SHARK',
     'COOKIECUTTER SHARK',
     'LARGETOOTH COOKIECUTTER SHARK',
     'POCKET SHARK',
     'SMALLEYE PYGMY SHARK',
     'SPINED PYGMY SHARK',
     'HOOKTOOTH DOGFISH',
     'HIGHFIN DOGFISH',
     'BLACK DOGFISH',
     'GRANULAR DOGFISH',
     'BARESKIN DOGFISH',
     'COMBTOOTH DOGFISH',
     'ORNATE DOGFISH',
     'WHITEFIN DOGFISH',
     'NEW ZEALAND LANTERNSHARK',
     'BLURRED LANTERNSHARK',
     'SHORT-TAIL LANTERNSHARK',
     'LINED LANTERNSHARK',
     'BROAD-SNOUT LANTERNSHARK',
     'CYLINDRICAL LANTERNSHARK',
     'TAILSPOT LANTERNSHARK',
     'BROWN LANTERNSHARK',
     'COMBTOOTH LANTERNSHARK',
     'PINK LANTERNSHARK',
     'LINED LANTERNSHARK',
     'BLACKMOUTH LANTERNSHARK',
     'PYGMY LANTERNSHARK',
     'BROADBANDED LANTERNSHARK',
     'SOUTHERN LANTERNSHARK',
     'CARIBBEAN LANTERNSHARK',
     'SHORTFIN SMOOTH LANTERNSHARK',
     'SMALLEYE LANTERNSHARK',
     'BLACKBELLY LANTERNSHARK',
     'SLENDERTAIL LANTERNSHARK',
     'DWARF LANTERNSHARK',
     'AFRICAN LANTERNSHARK',
     'GREAT LANTERNSHARK',
     'FALSE LANTERNSHARK',
     'SMOOTH LANTERNSHARK',
     'DENSE-SCALE LANTERNSHARK',
     'WEST INDIAN LANTERNSHARK',
     'FRINGEFIN LANTERNSHARK',
     'SCULPTED LANTERNSHARK',
     'THORNY LANTERNSHARK',
     'RASPTOOTH DOGFISH',
     'VELVET BELLY LANTERNSHARK',
     'SPLENDID LANTERNSHARK',
     'BRISTLED LANTERNSHARK',
     'BLUE-EYE LANTERNSHARK',
     'HAWAIIAN LANTERNSHARK',
     'GREEN LANTERNSHARK',
     'VIPER DOGFISH',
     'PRICKLY DOGFISH',
     'CARIBBEAN ROUGHSHARK',
     'ANGULAR ROUGHSHARK',
     'JAPANESE ROUGHSHARK',
     'SAILFIN ROUGHSHARK',
     'PORTUGUESE DOGFISH',
     'ROUGHSKIN DOGFISH',
     'LONGNOSE VELVET DOGFISH',
     'LARGESPINE VELVET DOGFISH',
     'PLUNKET SHARK',
     'WHITETAIL DOGFISH',
     'AZORES DOGFISH',
     'SPARSETOOTH DOGFISH',
     "SHERWOOD'S DOGFISH",
     'KNIFETOOTH DOGFISH',
     'SOUTHERN SLEEPER SHARK',
     'FROG SHARK',
     'GREENLAND SHARK',
     'PACIFIC SLEEPER SHARK',
     'LITTLE SLEEPER SHARK',
     'JAPANESE VELVET DOGFISH',
     'VELVET DOGFISH',
     'ROUGHSKIN SPURDOG',
     'SOUTHERN MANDARIN DOGFISH',
     'MANDARIN DOGFISH',
     'SPINY DOGFISH',
     'EASTERN HIGHFIN SPURDOG',
     'WESTERN HIGHFIN SPURDOG',
     'LONGNOSE SPURDOG',
     'JAPANESE SHORTNOSE SPURDOG',
     'BIGHEAD SPURDOG',
     'GREENEYE SPURDOG',
     'FATSPINE SPURDOG',
     'CUBAN DOGFISH',
     "EDMUND'S SPURDOG",
     'TAIWAN SPURDOG',
     'EASTERN LONGNOSE SPURDOG',
     'NORTHERN SPINY DOGFISH',
     'INDONESIAN SHORTSNOUT SPURDOG',
     'JAPANESE SPURDOG',
     'SEYCHELLES SPURDOG',
     'SHORTNOSE SPURDOG',
     'BLACKTAILED SPURDOG',
     'SHORTSPINE SPURDOG',
     'INDONESIAN GREENEYE SPURDOG',
     'WESTERN LONGNOSE SPURDOG',
     'BARTAIL SPURDOG',
     'CYRANO SPURDOG',
     'KERMADEC SPINY DOGFISH',
     'SPOTTED SPINY DOGFISH',
     'SAWBACK ANGELSHARK',
     'AFRICAN ANGELSHARK',
     'EASTERN ANGELSHARK',
     'ARGENTINE ANGELSHARK',
     'CHILEAN ANGELSHARK',
     'AUSTRALIAN ANGELSHARK',
     'PHILIPPINES ANGELSHARK',
     'PACIFIC ANGELSHARK',
     'SAND DEVIL',
     'TAIWAN ANGELSHARK',
     'ANGULAR ANGELSHARK',
     'GULF ANGELSHARK',
     'JAPANESE ANGELSHARK',
     'INDONESIAN ANGELSHARK',
     'MEXICAN ANGELSHARK',
     'CLOUDED ANGELSHARK',
     'HIDDEN ANGELSHARK',
     'SMOOTHBACK ANGELSHARK',
     'WESTERN ANGELSHARK',
     'ANGELSHARK',
     'ORNATE ANGELSHARK',
     'OCELLATED ANGELSHARK',
     'WHITE SHARK']
    
    count_1 = 0
    count_2 = 0
    check = False
    
    print(f"Limpiando columna *{df[col].name}*")
    
    #por cada registro de ataque
    for ind in tqdm(df.index):
        
        #por cada especie de la biblioteca
        for i in sharks_biblio:

            #miramos si la especie esta contenida en el campo especie
            if re.search(i, str(df[col][ind])):

                #si es asi la sobreescribimos con el nombre normalizado
                df.loc[ind,col] = i
                count_1 += 1
                check = True #pasamos a True el chivato "check"
                break #al haber encontrado coincidencia, rompemos bucle para que deje de iterar (control de bucle)

        if check == False: #si no ha encontrado coincidencia (el check no ha saltado)
            df.loc[ind,col]=np.nan
            count_2 += 1
        check = False #lo vuelve a poner a False
                    
    return f"{count_1} valores han sido substituidos. {count_2} se han catalogado como desconocidos"

def gender_clean(df, col):
    
    '''
    df : DataFrame
    col : nombre de la columna 1 a limpiar (string)
    
    La función sex_clean recibe como parametro un DataFrame y el nombre de la columna donde se encuentran los datos categoricos de genero
    El objetivo es que en la columna solo haya los siguientes tipos de valores-> F(Female), M(Male), NaN(Sin genero)
    
    1. Se quitan los datos espacios delante y detras de todos los valores
    2. Todos los datos no nulos se pasan a mayúsculas
    4. Todos los datos nulos se definen como NaN
    5. Todo lo que no sea 'F' o 'M' se define como NaN
    
    '''
    
    print(f"Limpiando la columna *{df[col].name}*")
    
    df[col] = df[col].str.upper().str.strip()
    
    count = 0
    
    for ind in tqdm(df.index):
        
        if df[col][ind] != 'M' and df[col][ind] != 'F':
            df.loc[ind, col] = np.nan
            count+=1
        
    return f"No se ha podido identificar el género de {count} registros"

def gender_catcher(df, col1, col2):
    
    '''
    df : DataFrame
    col1 : nombre de la columna 1 a limpiar (string)
    col2 : nombre de la columna 2 a limpiar (string)
    
    La función gender_catcher recibe como parametro un DataFrame, el nombre de la columna col1 donde se encuentran los datos
    categoricos de genero, y la columna col2 donde se encuentran los nombres.
    El objetivo es encontrar en algunos nombres, las palabras 'Female' y 'Male' y asi rellenar datos que quizas en la col1 sean nulos.
    
    1. Se pasan todas las strings de col2 a minusculas
    2. Se verifica registro por registro que tratamos con un string
    3. Se le aplica un filtro regex a la col2 para verificar que dentro de la cadena de caracteres exista la palabra 'female' o 'male'
    4. En el caso de que si exista, se agrega a la col1 la letra 'F' o 'M' segun corresponda
    5. En caso de que no existan ninguna de las dos palabras el valor en col1 se asignara como NaN
    
    '''
    print(f"Comparando las columnas *{df[col1].name}* y *{df[col2].name}*")
    
    df[col2] = df[col2].str.lower().str.strip()
    
    count = 0
    
    for ind in tqdm(df.index):
        if (re.search('\w*female', str(df[col2][ind])) != None) and (df[col1][ind] != 'F'):
            df.loc[ind,col1] = 'F'
            
            count += 1
        
        elif (re.search('(?<!fe)male', str(df[col2][ind])) != None) and (df[col1][ind] != 'M'):
            df.loc[ind,col1] = 'M'
            
            count += 1

    return f"Se han rescatado {count} registros"

def type_clean(df, col):
    
    '''
    df : DataFrame
    col : nombre de la columna a limpiar (string)
    
    Regulariza valores existentes en solo tres categorias-> Unprovoked, Provoked, Unknown
    Los nulos continuan nulos
    '''
    
    # Todo lo que no sea provoked, se considera unprovoked (lógica)
    unprovoked = ['Unprovoked','Sea Disaster','Boating','Boat','Boatomg']
    provoked = ['Provoked']
    
    print(f"Homogeneizando categorias columna *{df[col].name}*")
    
    count_1 = 0
    count_2 = 0
    count_3 = 0
    
    for ind in tqdm(df.index):
    
        if df[col][ind] in unprovoked: 
            
            df.loc[ind,col] = 'Unprovoked'
            count_1 += 1
            
        elif df[col][ind] in provoked: 
            
            df.loc[ind,col] = 'Provoked'
            count_2 += 1
            
        else: 
            
            df.loc[ind,col] = np.nan
            count_3 += 1
     
    return f"Unprovoked: {count_1}; Provoked: {count_2}; Nulos: {count_3}"
            
def age_clean(df,col):
    
    '''
    df : DataFrame
    col : nombre de la columna a limpiar (string)
    '''
    
    print(f"Limpiando la columna *{df[col].name}*")
    
    for ind in tqdm(df.index):
        
        try:
    
            if isinstance(df[col][ind],int): 
                pass
            
            elif isinstance(df[col][ind],float): 
                df.loc[ind,col]=int(df[col][ind])
                
            elif isinstance(df[col][ind],str): 
                df.loc[ind,col]=int(df[col][ind].strip())
                
            else: 
                df.loc[ind,col]=np.nan
            
        except ValueError: 
            df.loc[ind,col] = np.nan
            
    return 