from io import StringIO
import pandas as pd

import pandas as pd
from io import StringIO

def get_mobile_specs_data():
    """
    Returns the mobile specifications data as a pandas DataFrame.
    Data updated based on provided commercial names, with specifications 
    fetched/verified based on those names (common configurations shown).
    
    Note: RAM/Storage may represent one common variant. Sensor data and
    Android version reflect typical specifications for the model. 
    'Background Task Killing Tendency' is a subjective assessment based 
    on brand/OS reputation (High=Aggressive, Moderate=Balanced, Low=Minimal).

    Returns:
        pandas.DataFrame: Mobile specifications data
    """
    # Define the mobile specs table as CSV (Updated 2024-07-26 based on provided mappings)
    mobile_specs_csv = """Original Model,Brand,Device Name,Release Year,Android Version,Fingerprint Sensor,Accelerometer,Gyro,Proximity Sensor,Compass,Barometer,Background Task Killing Tendency,Chipset,RAM,Storage,Battery (mAh)
220733SFG,Xiaomi,Xiaomi Redmi A1+,2022,12 (Go edition),True,True,False,True,False,False,High,MediaTek Helio A22,2GB,32GB,5000
23028RNCAG,Xiaomi,Xiaomi Redmi A2+,2023,13 (Go edition),True,True,False,True,False,False,High,MediaTek Helio G36,2GB,32GB,5000
23106RN0DA,Xiaomi,Xiaomi Redmi 13C,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
23129RAA4G,Xiaomi,Xiaomi Redmi Note 13 4G,2024,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 685,6GB,128GB,5000
23129RN51X,Xiaomi,Xiaomi Redmi A3,2024,14 (Go edition),True,True,False,True,True,False,High,MediaTek Helio G36,3GB,64GB,5000
2409BRN2CA,Xiaomi,Xiaomi Redmi 14C,2024,14,True,True,False,True,True,False,High,MediaTek Helio G91 Ultra,4GB,128GB,5030
BKK-LX2,Honor,Honor 8C,2018,8.1 (Oreo),True,True,False,True,True,False,High,Qualcomm Snapdragon 632,4GB,32GB,4000
CPH1729,Oppo,Oppo A83,2017,7.1 (Nougat),False,True,False,True,True,False,High,MediaTek Helio P23,3GB,32GB,3180
CPH1823,Oppo,Oppo F9 (F9 Pro),2018,8.1 (Oreo),True,True,False,True,True,False,High,MediaTek Helio P60,4GB,64GB,3500
CPH1909,Oppo,Oppo A5s,2019,8.1 (Oreo),True,True,False,True,True,False,High,MediaTek Helio P35,3GB,32GB,4230
CPH1911,Oppo,Oppo F11,2019,9.0 (Pie),True,True,False,True,True,False,High,MediaTek Helio P70,4GB,128GB,4020
CPH1923,Oppo,Oppo A1k,2019,9.0 (Pie),False,True,False,True,True,False,High,MediaTek Helio P22,2GB,32GB,4000
CPH1989,Oppo,Oppo Reno2 F,2019,9.0 (Pie),True,True,True,True,True,False,High,MediaTek Helio P70,8GB,128GB,4000
CPH2015,Oppo,Oppo A31,2020,9.0 (Pie),True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,4230
CPH2095,Oppo,Oppo F17,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 662,4GB,64GB,4015
CPH2121,Oppo,Oppo A93,2020,10,True,True,True,True,True,False,High,MediaTek Helio P95,8GB,128GB,4000
CPH2127,Oppo,Oppo A53,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 460,4GB,64GB,5000
CPH2159,Oppo,Oppo Reno5 4G,2020,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,8GB,128GB,4310
CPH2185,Oppo,Oppo A15,2020,10,True,True,False,True,True,False,High,MediaTek Helio P35,2GB,32GB,4230
CPH2203,OPPO,OPPO A94,2021,11,True,True,True,True,True,False,High,MediaTek Helio P95,8GB,128GB,4310
CPH2219,Oppo,Oppo A74,2021,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 662,4GB,128GB,5000
CPH2239,Oppo,Oppo A54,2021,10,True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,5000
CPH2325,Oppo,Oppo A55,2021,11,True,True,False,True,True,False,High,MediaTek Helio G35,4GB,64GB,5000
CPH2333,Oppo,Oppo A96,2022,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
CPH2477,Oppo,Oppo A17,2022,12,True,True,False,True,False,False,High,MediaTek Helio G35,4GB,64GB,5000
CPH2481,OPPO,OPPO Reno8 T,2023,13,True,True,True,True,True,False,High,MediaTek Helio G99,8GB,128GB,5000
CPH2565,Oppo,Oppo A78 4G,2023,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
CPH2579,Oppo,Oppo A38,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
CPH2591,Oppo,Oppo A18,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,64GB,5000
CPH2631,Oppo,Oppo A60,2024,14,True,True,True,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
CPH2637,OPPO,OPPO Reno 12 F 5G,2024,14,True,True,True,True,True,False,High,MediaTek Dimensity 6300,8GB,256GB,5000
CPH2669,Oppo,Oppo A3 4G,2024,14,True,True,True,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
Infinix X652A,Infinix,Infinix S5,2019,9.0 (Pie),True,True,False,True,True,False,High,MediaTek Helio P22,4GB,64GB,4000
Infinix X656,Infinix,Infinix Note 7 Lite,2020,10,True,True,False,True,True,False,High,MediaTek Helio G70,4GB,64GB,5000
Infinix X657C,Infinix,Infinix Smart 5,2020,10 (Go edition),True,True,False,True,False,False,High,MediaTek Helio A20,2GB,32GB,5000
Infinix X680,Infinix,Infinix Hot 9 Play,2020,10 (Go edition),True,True,False,True,False,False,High,MediaTek Helio A22 / A25,2GB,32GB,6000
Infinix X653,Infinix,Infinix Smart 4,2019,9.0 (Pie Go edition),True,True,False,True,False,False,High,MediaTek Helio A22,1GB,16GB,4000
itel A665L,itel,iTel A70,2023,13 (Go edition),True,True,False,True,False,False,High,Unisoc T603,3GB,64GB,5000
JKM-LX1,Huawei,Huawei Y9 (2019),2018,8.1 (Oreo),True,True,True,True,True,False,High,Hisilicon Kirin 710,3GB,64GB,4000
MRD-LX1F,Huawei,Huawei Y6 (2019),2019,9.0 (Pie),False,True,False,True,True,False,High,MediaTek Helio A22,2GB,32GB,3020
STK-LX1,Honor,Honor 9X,2019,9.0 (Pie),True,True,True,True,True,False,High,Hisilicon Kirin 710F,4GB,64GB,4000
ELI-NX9,Honor,Honor 200 5G,2024,14,True,True,True,True,True,False,High,Qualcomm Snapdragon 7 Gen 3,8GB,256GB,5200
M2006C3MG,Xiaomi,Xiaomi Redmi 9C,2020,10,True,True,False,True,False,False,High,MediaTek Helio G35,2GB,32GB,5000
M2007J20CG,Xiaomi,Xiaomi Poco X3 NFC,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 732G,6GB,64GB,5160
M2101K6G,Xiaomi,Xiaomi Redmi Note 10 Pro,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 732G,6GB,64GB,5020
M2101K7BG,Xiaomi,Xiaomi Redmi Note 10S,2021,11,True,True,True,True,True,False,High,MediaTek Helio G95,6GB,64GB,5000
M2102J20SG,Xiaomi,Xiaomi Poco X3 Pro,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 860,6GB,128GB,5160
Pixel 6,Google,Google Pixel 6,2021,12,True,True,True,True,True,True,Low,Google Tensor G1,8GB,128GB,4614
Redmi K20 Pro,Xiaomi,Xiaomi Redmi K20 Pro,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 855,6GB,64GB,4000
Redmi Note 7,Xiaomi,Xiaomi Redmi Note 7,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 660,3GB,32GB,4000
Redmi Note 8,Xiaomi,Xiaomi Redmi Note 8,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 665,3GB,32GB,4000
Redmi Note 9S,Xiaomi,Xiaomi Redmi Note 9S,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,4GB,64GB,5020
RMX2040,Realme,Realme 6i,2020,10,True,True,True,True,True,False,High,MediaTek Helio G80,3GB,64GB,5000
RMX2085,Realme,Realme X3,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 855+,6GB,128GB,4200
RMX2180,Realme,Realme C15,2020,10,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,6000
RMX2185,Realme,Realme C11,2020,10,False,True,False,True,True,False,High,MediaTek Helio G35,2GB,32GB,5000
RMX2189,Realme,Realme C12,2020,10,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,6000
RMX3231,Realme,Realme C11 (2021),2021,11 (Go edition),False,True,False,True,True,False,High,Unisoc SC9863A,2GB,32GB,5000
RMX3261,Realme,Realme C21Y,2021,10,True,True,False,True,True,False,High,Unisoc T610,3GB,32GB,5000
RMX3263,Realme,Realme C21Y,2021,10,True,True,False,True,True,False,High,Unisoc T610,4GB,64GB,5000
RMX3269,Realme,Realme C25Y,2021,11,True,True,False,True,True,False,High,Unisoc T610,4GB,64GB,5000
RMX3363,Realme,Realme GT Master,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 778G 5G,6GB,128GB,4300
RMX3627,Realme,Realme C33 2023,2023,12,True,True,False,True,True,False,High,Unisoc Tiger T612,4GB,64GB,5000
RMX3710,Realme,Realme C55,2023,13,True,True,False,True,True,False,High,MediaTek Helio G88,6GB,128GB,5000
RMX3834,Realme,Realme Note 50,2024,13 (Go edition),True,True,False,True,True,False,High,Unisoc Tiger T612,3GB,64GB,5000
RMX3890,Realme,Realme C67,2023,14,True,True,False,True,True,False,High,Qualcomm Snapdragon 685,8GB,128GB,5000
RMX3910,Realme,Realme C65,2024,14,True,True,False,True,True,False,High,MediaTek Helio G85,6GB,128GB,5000
RMX3939,Realme,Realme C63,2024,14,True,True,False,True,True,False,High,Unisoc Tiger T612,6GB,128GB,5000
RMX3997,Realme,Realme C65 5G,2024,14,True,True,False,True,True,False,High,MediaTek Dimensity 6300,4GB,64GB,5000
SM-A022F,Samsung,Samsung Galaxy A02,2021,10,False,True,False,True,False,False,Moderate,MediaTek MT6739W,2GB,32GB,5000
SM-A025F,Samsung,Samsung Galaxy A02s,2020,10,False,True,False,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,5000
SM-A032F,Samsung,Samsung Galaxy A03 Core,2021,11 (Go edition),False,True,False,True,False,False,Moderate,Unisoc SC9863A,2GB,32GB,5000
SM-A057F,Samsung,Samsung Galaxy A05s,2023,13,True,True,False,True,False,False,Moderate,Qualcomm Snapdragon 680 4G,4GB,64GB,5000
SM-A107F,Samsung,Samsung Galaxy A10s,2019,9.0 (Pie),True,True,False,True,False,False,Moderate,MediaTek Helio P22,2GB,32GB,4000
SM-A125F,Samsung,Samsung Galaxy A12,2020,10,True,True,False,True,False,False,Moderate,MediaTek Helio P35,3GB,32GB,5000
SM-A137F,Samsung,Samsung Galaxy A13,2022,12,True,True,False,True,True,False,Moderate,Exynos 850,4GB,64GB,5000
SM-A155F,Samsung,Samsung Galaxy A15,2023,14,True,True,False,True,True,False,Moderate,MediaTek Helio G99,4GB,128GB,5000
SM-A205F,Samsung,Samsung Galaxy A20,2019,9.0 (Pie),True,True,True,True,True,False,Moderate,Exynos 7884,3GB,32GB,4000
SM-A217F,Samsung,Samsung Galaxy A21s,2020,10,True,True,True,True,False,False,Moderate,Exynos 850,3GB,32GB,5000
SM-A235F,Samsung,Samsung Galaxy A23,2022,12,True,True,False,True,True,False,Moderate,Qualcomm Snapdragon 680 4G,4GB,64GB,5000
SM-A245F,Samsung,Samsung Galaxy A24 4G,2023,13,True,True,True,True,True,False,Moderate,MediaTek Helio G99,4GB,128GB,5000
SM-A305F,Samsung,Samsung Galaxy A30,2019,9.0 (Pie),True,True,True,True,True,False,Moderate,Exynos 7904,3GB,32GB,4000
SM-A325F,Samsung,Samsung Galaxy A32,2021,11,True,True,True,True,True,False,Moderate,MediaTek Helio G80,4GB,64GB,5000
SM-A515F,Samsung,Samsung Galaxy A51,2019,10,True,True,True,True,True,False,Moderate,Exynos 9611,4GB,64GB,4000
SM-A750F,Samsung,Samsung Galaxy A7 (2018),2018,8.0 (Oreo),True,True,True,True,True,False,Moderate,Exynos 7885,4GB,64GB,3300
SM-M115F,Samsung,Samsung Galaxy M11,2020,10,True,True,False,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,5000
SM-M127F,Samsung,Samsung Galaxy M12,2021,11,True,True,False,True,False,False,Moderate,Exynos 850,4GB,64GB,6000
TECNO BG6,Tecno,Tecno Spark Go 2024,2023,13 (Go edition),True,True,False,True,False,False,High,Unisoc T606,3GB,64GB,5000
V2026,Vivo,vivo Y12s,2020,10,True,True,False,True,True,False,High,MediaTek Helio P35,3GB,32GB,5000
V2061,Vivo,vivo V21e,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,8GB,128GB,4000
V2120,Vivo,vivo Y15s,2021,11 (Go edition),True,True,False,True,True,False,High,MediaTek Helio P35,3GB,32GB,5000
V2207,Vivo,vivo Y22,2022,12,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,64GB,5000
V2247,Vivo,vivo Y36,2023,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
RMX3085,Realme,Realme 8,2021,11,True,True,True,True,True,False,High,MediaTek Helio G95,4GB,64GB,5000
V2352,Vivo,vivo Y28 4G,2024,14,True,True,False,True,True,False,High,MediaTek Helio G85,8GB,128GB,6000
RMX3760,Realme,Realme C53,2023,13,True,True,True,True,True,False,High,Unisoc Tiger T612,6GB,128GB,5000
23053RN02A,Xiaomi,Xiaomi Redmi 12,2023,13,True,True,False,True,True,False,High,MediaTek Helio G88,4GB,128GB,5000
SM-A207F,Samsung,Samsung Galaxy A20s,2019,9.0 (Pie),True,True,True,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,4000
CPH2641,OPPO,OPPO A3x,2024,14,True,True,False,True,True,False,High,MediaTek Dimensity 6300,6GB,128GB,5000
SM-A135F,Samsung,Samsung Galaxy A13,2022,12,True,True,False,True,True,False,Moderate,MediaTek Helio G80,4GB,64GB,5000
SM-A326B,Samsung,Samsung Galaxy A32 5G,2021,11,True,True,True,True,True,False,Moderate,MediaTek Dimensity 720 5G,4GB,64GB,5000
M2004J19C,Xiaomi,Xiaomi Redmi 9,2020,10,True,True,False,True,True,False,High,MediaTek Helio G80,3GB,32GB,5020
CPH2349,OPPO,OPPO A16k,2021,11,False,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,4230
CPH2471,Oppo,Oppo A17K,2022,12,True,True,False,True,False,False,High,MediaTek Helio G35,3GB,64GB,5000
SM-A260F,Samsung,Samsung Galaxy A2 Core,2019,8.1 (Oreo Go edition),False,True,False,True,False,False,Moderate,Exynos 7870 Octa,1GB,8GB,2600
M2003J15SC,Xiaomi,Xiaomi Redmi Note 9,2020,10,True,True,True,True,True,False,High,MediaTek Helio G85,3GB,64GB,5020
STK-L21,Huawei,Huawei Y9 Prime (2019),2019,9.0 (Pie),True,True,True,True,True,False,High,Hisilicon Kirin 710F,4GB,64GB,4000
ATU-L31,Huawei,Huawei Y6 Prime (2018),2018,8.0 (Oreo),False,True,False,True,False,False,High,Qualcomm Snapdragon 425,2GB,16GB,3000
SM-A105F,Samsung,Samsung Galaxy A10,2019,9.0 (Pie),False,True,False,True,False,False,Moderate,Exynos 7884,2GB,32GB,3400
V2027,Vivo,vivo Y20i,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 460,3GB,64GB,5000
Infinix X6525B,Infinix,Infinix Smart 8,2023,13,True,True,False,True,True,False,High,Unisoc T606,3GB,64GB,5000
RMX3830,Realme,Realme C51,2023,13,True,True,False,True,True,False,High,Unisoc Tiger T612,4GB,64GB,5000
CPH1931,Oppo,Oppo A5 (2020),2019,9.0 (Pie),True,True,False,True,True,False,High,Qualcomm Snapdragon 665,3GB,64GB,5000
21121119SG,Xiaomi,Xiaomi Redmi 10 (2022),2022,11,True,True,False,True,True,False,High,MediaTek Helio G88,4GB,64GB,5000
2201117SG,Xiaomi,Xiaomi Redmi Note 11S,2022,11,True,True,True,True,True,False,High,MediaTek Helio G96,6GB,64GB,5000
23108RN04Y,Xiaomi,Xiaomi Redmi 13C,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
ALE-L21,Huawei,HUAWEI P8 lite,2015,5.0.2 (Lollipop),False,True,False,True,True,False,High,Hisilicon Kirin 620,2GB,16GB,2200
CPH2061,Oppo,Oppo A52,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 665,4GB,64GB,5000
CPH2113,Oppo,Oppo Reno4,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,8GB,128GB,4015
CPH2179,Oppo,Oppo A15s,2020,10,True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,4230
CPH2269,Oppo,Oppo A16,2021,11,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,5000
Infinix X6511B,Infinix,Infinix Smart 6,2021,11 (Go edition),True,True,False,True,False,False,High,Unisoc SC9863A,2GB,32GB,5000
Infinix X669D,Infinix,Infinix Hot 30i,2023,12,True,True,False,True,True,False,High,Unisoc T606,4GB,64GB,5000
JSN-L22,Honor,Honor 8X,2018,8.1 (Oreo),True,True,True,True,True,False,High,Hisilicon Kirin 710,4GB,64GB,3750
M2006C3LG,Xiaomi,Xiaomi Redmi 9A,2020,10,False,True,False,True,False,False,High,MediaTek Helio G25,2GB,32GB,5000
REA-NX9,Honor,Honor 90 5G,2023,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 7 Gen 1 Accelerated Edition,8GB,256GB,5000
RMX1941,Realme,Realme C2,2019,9.0 (Pie),False,True,False,True,True,False,High,MediaTek Helio P22,2GB,16GB,4000
RMX2020,Realme,Realme C3,2020,10,True,True,False,True,True,False,High,MediaTek Helio G70,2GB,32GB,5000
SM-A047F,Samsung,Samsung Galaxy A04s,2022,12,True,True,False,True,False,False,Moderate,Exynos 850,3GB,32GB,5000
SM-A145F,Samsung,Samsung Galaxy A14,2023,13,True,True,False,True,True,False,Moderate,MediaTek Helio G80 / Exynos 850,4GB,64GB,5000
SM-M315F,Samsung,Samsung Galaxy M31,2020,10,True,True,True,True,True,False,Moderate,Exynos 9611,6GB,64GB,6000
SM-N9700,Samsung,Samsung Galaxy Note10,2019,9.0 (Pie),True,True,True,True,True,True,Moderate,Exynos 9825 / Snapdragon 855,8GB,256GB,3500
TECNO BB2,Tecno,Tecno Pop 3 Plus,2019,9.0 (Pie Go edition),True,True,False,True,False,False,High,MediaTek Helio A22,1GB,16GB,4000
TFY-LX2,Honor,Honor X8,2022,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,6GB,128GB,4000
V2029,Vivo,vivo Y20,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 460,3GB,64GB,5000
V2111-EG,Vivo,vivo Y21,2021,11,True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,5000
V2203,Vivo,Vivo Y02S,2022,12 (Go edition),False,True,False,True,False,False,High,MediaTek Helio P35,2GB,32GB,5000
vivo 2015,Vivo,vivo Y1s,2020,10,False,True,False,True,False,False,High,MediaTek Helio P35,2GB,32GB,4030
V2434,Vivo,Vivo Y29,Not Released/Info Unavailable,?,False,False,False,False,False,False,High,?,?GB,?GB,?
"""
    
    # Read mobile specs into a DataFrame
    # Using StringIO to simulate reading from a file
    return pd.read_csv(StringIO(mobile_specs_csv))



def merge_with_mobile_specs(df):
    """
    Merge a DataFrame with the mobile specifications data and set default values for missing models.
    
    Args:
        df: pandas.DataFrame with a 'model' column to merge on
        
    Returns:
        pandas.DataFrame: Merged DataFrame with mobile specifications, including default values for missing models
    """
    mobile_specs_df = get_mobile_specs_data()
    
    # Define default values for each column based on data types
    default_values = {
        'Original Model': '',
        'Brand': 'Unknown',
        'Device Name': 'Unknown Device',
        'Release Year': 2000,
        'Android Version': 'Unknown',
        'Fingerprint Sensor': False,
        'Accelerometer': False,
        'Gyro': False,
        'Proximity Sensor': False,
        'Compass': False,
        'Barometer': False,
        'Background Task Killing Tendency': 'High',
        'Chipset': 'Unknown',
        'RAM': '2GB',
        'Storage': '16GB',
        'Battery (mAh)': 3000
    }
    
    # Merge the data with mobile specs using 'model' from exported data and 'Original Model' from specs
    merged_df = pd.merge(df, mobile_specs_df, left_on='model', right_on='Original Model', how='left')
    
    # Fill missing values with defaults
    for column, default_value in default_values.items():
        if column in merged_df.columns:
            merged_df[column].fillna(default_value, inplace=True)
            
    return merged_df 