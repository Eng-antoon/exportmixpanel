from io import StringIO
import pandas as pd

def get_mobile_specs_data():
    """
    Returns the mobile specifications data as a pandas DataFrame.
    
    Returns:
        pandas.DataFrame: Mobile specifications data
    """
    # Define the mobile specs table as CSV
    mobile_specs_csv = """Original Model,Brand,Device Name,Release Year,Android Version,Fingerprint Sensor,Accelerometer,Gyro,Proximity Sensor,Compass,Barometer,Background Task Killing Tendency,Chipset,RAM,Storage,Battery (mAh)
220733SFG,Xiaomi,Xiaomi 12 Lite,2022,12,True,True,True,True,True,False,High,Qualcomm Snapdragon 778G 5G,8GB,128GB,4300
23028RNCAG,Xiaomi,Xiaomi 13 Lite,2023,12,True,True,True,True,True,False,High,Qualcomm Snapdragon 7 Gen 1,8GB,128GB,4500
23106RN0DA,Xiaomi,Redmi 13C,2023,13,False,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
23129RAA4G,Xiaomi,Redmi Note 13 Pro 5G,2023,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 7s Gen 2,8GB,256GB,5100
23129RN51X,Xiaomi,Redmi Note 13 Pro+ 5G,2023,13,True,True,True,True,True,False,High,MediaTek Dimensity 7200-Ultra,8GB,256GB,5000
2409BRN2CA,Xiaomi,Redmi 13,2024,14,False,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5030
BKK-LX2,Huawei,Y7 2019,2019,8.1 (Oreo),False,True,False,True,True,False,High,Qualcomm Snapdragon 450,3GB,32GB,4000
CPH1729,Oppo,A71 (2017),2017,7.1 (Nougat),False,True,False,True,True,False,High,MediaTek MT6750,2GB,16GB,3000
CPH1823,Oppo,A3s,2018,8.1 (Oreo),False,True,False,True,True,False,High,Qualcomm Snapdragon 450,2GB,16GB,4230
CPH1909,Oppo,A5 (2020),2020,9.0 (Pie),True,True,False,True,True,False,High,Qualcomm Snapdragon 665,3GB,64GB,5000
CPH1911,Oppo,A9 (2020),2019,9.0 (Pie),True,True,False,True,True,False,High,Qualcomm Snapdragon 665,4GB,128GB,5000
CPH1923,Oppo,A31,2020,9.0 (Pie),True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,4230
CPH1989,Oppo,A91,2020,9.0 (Pie),True,True,True,True,True,False,High,MediaTek Helio P70,8GB,128GB,4025
CPH2015,Oppo,A52,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 665,4GB,64GB,5000
CPH2095,Oppo,A73 5G,2020,10,True,True,False,True,True,False,High,MediaTek Dimensity 720,4GB,64GB,4040
CPH2121,Oppo,Reno5 4G,2020,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,8GB,128GB,4310
CPH2127,Oppo,A53,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 460,4GB,64GB,5000
CPH2159,Oppo,A74 4G,2021,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 662,4GB,64GB,5000
CPH2185,Oppo,Reno6 5G,2021,11,True,True,True,True,True,False,High,MediaTek Dimensity 900 5G,8GB,128GB,4300
CPH2203,Oppo,A15s,2020,10,True,True,False,True,True,False,High,MediaTek Helio P35,3GB,32GB,4230
CPH2219,Oppo,A54,2021,10,True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,5000
CPH2239,Oppo,A16,2021,11,True,True,False,True,True,False,High,MediaTek Helio G35,4GB,64GB,5000
CPH2325,Oppo,A96,2022,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
CPH2333,Oppo,A76,2022,11.3,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,4GB,128GB,5000
CPH2477,Oppo,A77 5G,2022,12.1,True,True,False,True,True,False,High,MediaTek Dimensity 810,4GB,64GB,5000
CPH2481,Oppo,A57 (2022),2022,12.1,True,True,False,True,True,False,High,MediaTek Helio G35,4GB,64GB,5000
CPH2565,Oppo,A78 5G,2023,13,True,True,False,True,True,False,High,MediaTek Dimensity 700,4GB,128GB,5000
CPH2579,Oppo,A58 4G,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,6GB,128GB,5000
CPH2591,Oppo,A38,2023,13.1,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,64GB,5000
CPH2631,Oppo,A2 Pro 5G,2023,13,True,True,True,True,True,False,High,MediaTek Dimensity 7050,8GB,256GB,5000
CPH2637,Oppo,A2m,2023,13,True,True,False,True,True,False,High,MediaTek Dimensity 6020,4GB,128GB,5000
CPH2669,Oppo,A3x 5G,2024,14,True,True,False,True,True,False,High,MediaTek Dimensity 6300,8GB,128GB,5000
Infinix X652A,Infinix,Smart 5 Pro,2021,11,True,True,False,True,False,False,High,Unisoc SC9863A,2GB,32GB,6000
Infinix X656,Infinix,Hot 10 Play,2021,10,True,True,False,True,False,False,High,MediaTek Helio G25/G35,2GB,32GB,6000
Infinix X657C,Infinix,Smart 6 HD,2022,Android 11 (Go edition),False,True,False,True,False,False,High,Unisoc SC9863A,2GB,32GB,5000
Infinix X680,Infinix,Note 7,2020,10,True,True,True,True,True,False,High,MediaTek Helio G70,6GB,64GB,5000
Infinix X653,Infinix,Smart 5,2020,10,True,True,False,True,False,False,High,MediaTek Helio A20,2GB,32GB,5000
itel A665L,itel,A27,2022,Android 11 (Go edition),False,True,False,False,False,False,High,Unisoc SC9832E,2GB,32GB,4000
JKM-LX1,Huawei,P Smart (2019),2019,9.0 (Pie),True,True,False,True,True,False,High,Hisilicon Kirin 710,3GB,32GB,3400
MRD-LX1F,Huawei,Y6p,2020,10,True,True,False,True,True,False,High,MediaTek Helio P22,3GB,32GB,5000
STK-LX1,Huawei,Y7p,2020,9.0 (Pie),True,True,False,True,True,False,High,Hisilicon Kirin 710F,4GB,64GB,4000
ELI-NX9,Huawei,P40 Pro+,2020,10,True,True,True,True,True,True,High,Hisilicon Kirin 990 5G,8GB,256GB,4200
M2006C3MG,Xiaomi,Redmi 9C,2020,10,True,True,False,True,False,False,High,MediaTek Helio G35,2GB,32GB,5000
M2007J20CG,Xiaomi,Poco M3,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 662,4GB,64GB,6000
M2101K6G,Xiaomi,Poco X3 Pro,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 860,6GB,128GB,5160
M2101K7BG,Xiaomi,Redmi Note 10 Pro (India),2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 732G,6GB,64GB,5020
M2102J20SG,Xiaomi,Redmi Note 10S,2021,11,True,True,True,True,True,False,High,MediaTek Helio G95,6GB,64GB,5000
Pixel 6,Google,Pixel 6,2021,12,True,True,True,True,True,True,Low,Google Tensor G1,8GB,128GB,4614
Redmi K20 Pro,Xiaomi,Redmi K20 Pro,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 855,6GB,64GB,4000
Redmi Note 7,Xiaomi,Redmi Note 7,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 660,3GB,32GB,4000
Redmi Note 8,Xiaomi,Redmi Note 8,2019,9.0 (Pie),True,True,True,True,True,False,High,Qualcomm Snapdragon 665,4GB,64GB,4000
Redmi Note 9S,Xiaomi,Redmi Note 9S,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,4GB,64GB,5020
RMX2040,Realme,C11 (2020),2020,10,False,True,False,True,True,False,High,MediaTek Helio G35,2GB,32GB,5000
RMX2085,Realme,C21,2021,10,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,5000
RMX2180,Realme,7i (India),2020,10,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,64GB,6000
RMX2185,Realme,Narzo 20,2020,10,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,64GB,6000
RMX2189,Realme,C15,2020,10,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,6000
RMX3231,Realme,C25Y,2021,11,True,True,False,True,True,False,High,Unisoc Tiger T610,4GB,64GB,5000
RMX3261,Realme,C21Y,2021,11,True,True,False,True,True,False,High,Unisoc T610,3GB,32GB,5000
RMX3263,Realme,Narzo 50i,2021,11 (Go edition),False,True,False,True,False,False,High,Unisoc SC9863A,2GB,32GB,5000
RMX3269,Realme,C25s,2021,11,True,True,False,True,True,False,High,MediaTek G85,4GB,64GB,6000
RMX3363,Realme,9 Pro 5G,2022,12,True,True,True,True,True,False,High,Qualcomm Snapdragon 695 5G,6GB,128GB,5000
RMX3627,Realme,C30s,2022,Android 12 (Go edition),True,True,False,True,False,False,High,Unisoc SC9863A2,2GB,32GB,5000
RMX3710,Realme,C55,2023,13,True,True,False,True,True,False,High,MediaTek G88,6GB,128GB,5000
RMX3834,Realme,Narzo N53,2023,13,True,True,False,True,True,False,High,Unisoc Tiger T612,4GB,64GB,5000
RMX3890,Realme,Narzo 60 5G,2023,13,True,True,True,True,True,False,High,MediaTek Dimensity 6020,8GB,128GB,5000
RMX3910,Realme,C67 5G,2023,13,True,True,False,True,True,False,High,MediaTek Dimensity 6100+,4GB,128GB,5000
RMX3939,Realme,C65,2024,14,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
RMX3997,Realme,Narzo N55,2023,13,True,True,False,True,True,False,High,MediaTek Helio G88,4GB,64GB,5000
SM-A022F,Samsung,Galaxy A02,2021,10,False,True,False,True,False,False,Moderate,MediaTek MT6739W,2GB,32GB,5000
SM-A025F,Samsung,Galaxy A02s,2020,10,False,True,False,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,5000
SM-A032F,Samsung,Galaxy A03 Core,2021,Android 11 (Go edition),False,True,False,True,False,False,Moderate,Unisoc SC9863A,2GB,32GB,5000
SM-A057F,Samsung,Galaxy A05s,2023,13,True,True,False,True,False,False,Moderate,Exynos 850,4GB,64GB,5000
SM-A107F,Samsung,Galaxy A10s,2019,9.0 (Pie),True,True,False,True,False,False,Moderate,MediaTek Helio P22,2GB,32GB,4000
SM-A125F,Samsung,Galaxy A12,2020,10,True,True,False,True,False,False,Moderate,MediaTek Helio P35,4GB,64GB,5000
SM-A137F,Samsung,Galaxy A13 (4G),2022,12,True,True,True,True,True,False,Moderate,Exynos 850,4GB,64GB,5000
SM-A155F,Samsung,Galaxy A15 (5G),2023,14,True,True,True,True,True,False,Moderate,MediaTek Dimensity 6100+,4GB,128GB,5000
SM-A205F,Samsung,Galaxy A20,2019,9.0 (Pie),True,True,True,True,True,False,Moderate,Exynos 7884,3GB,32GB,4000
SM-A217F,Samsung,Galaxy A21s,2020,10,True,True,False,True,False,False,Moderate,Exynos 850,4GB,64GB,5000
SM-A235F,Samsung,Galaxy A23 (4G),2022,12,True,True,False,True,True,False,Moderate,Qualcomm Snapdragon 680 4G,4GB,64GB,5000
SM-A245F,Samsung,Galaxy A24 (4G),2023,13,True,True,False,True,True,False,Moderate,MediaTek Helio G99,6GB,128GB,5000
SM-A305F,Samsung,Galaxy A30,2019,9.0 (Pie),True,True,True,True,True,False,Moderate,Exynos 7904,4GB,64GB,4000
SM-A325F,Samsung,Galaxy A32 (4G),2021,11,True,True,True,True,True,False,Moderate,MediaTek Helio G80,4GB,64GB,5000
SM-A515F,Samsung,Galaxy A51,2019,10,True,True,True,True,True,False,Moderate,Exynos 9611,4GB,64GB,4000
SM-A750F,Samsung,Galaxy A7 (2018),2018,8.0 (Oreo),True,True,True,True,True,False,Moderate,Exynos 7885,4GB,64GB,3300
SM-M115F,Samsung,Galaxy M11,2020,10,True,True,False,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,5000
SM-M127F,Samsung,Galaxy M12,2021,11,True,True,False,True,False,False,Moderate,Exynos 850,4GB,64GB,6000
TECNO BG6,Tecno,Spark Go 2020,2020,Android 10 (Go edition),False,True,False,True,False,False,High,MediaTek Helio A20,2GB,32GB,4000
V2026,Vivo,Y20s (2020),2020,10.5,True,True,True,True,True,False,High,Qualcomm Snapdragon 460,4GB,128GB,5000
V2061,Vivo,Y51 (2020),2020,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 665,4GB,128GB,5000
V2120,Vivo,Y72 5G,2021,11,True,True,True,True,True,False,High,MediaTek Dimensity 700 5G,8GB,128GB,5000
V2207,Vivo,Y55 5G,2022,12,True,True,True,True,True,False,High,MediaTek Dimensity 700 5G,4GB,128GB,5000
V2247,Vivo,Y02t,2023,13,False,True,False,True,False,False,High,MediaTek Helio P35,4GB,64GB,5000
RMX3085,Realme,C21,2021,10,True,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,5000
V2352,Vivo,Y27 4G,2023,13,True,True,False,True,True,False,High,MediaTek Helio G85,4GB,128GB,5000
RMX3760,Realme,Realme C53,2023,13,True,True,False,True,True,False,High,Unisoc Tiger T612,6GB,128GB,5000
23053RN02A,Xiaomi,Redmi 12,2023,13,True,True,True,True,True,False,High,Qualcomm Snapdragon 4 Gen 2,8GB,128GB,5000
SM-A207F,Samsung,Galaxy A20s,2019,9.0 (Pie),True,True,False,True,False,False,Moderate,Qualcomm Snapdragon 450,3GB,32GB,4000
CPH2641,Oppo,A3 Pro,2024,14,True,True,True,True,True,False,High,MediaTek Dimensity 7050,8GB,256GB,5000
SM-A135F,Samsung,Galaxy A13 (5G),2021,11,True,True,False,True,False,False,Moderate,MediaTek Dimensity 700,4GB,64GB,5000
SM-A326B,Samsung,Galaxy A32 5G,2021,11,True,True,True,True,False,False,Moderate,MediaTek Dimensity 720 5G,4GB,64GB,5000
M2004J19C,Xiaomi,Redmi Note 9 Pro,2020,10,True,True,True,True,True,False,High,Qualcomm Snapdragon 720G,6GB,64GB,5020
CPH2349,Oppo,Reno7 4G,2022,11.1,True,True,True,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,4500
CPH2471,Oppo,A77s,2022,12.1,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
SM-A260F,Samsung,Galaxy A2 Core,2019,8.0 (Oreo) (Go edition),False,True,False,True,False,False,Moderate,Exynos 7870 Octa,1GB,16GB,2600
M2003J15SC,Xiaomi,Redmi 9,2020,10,False,True,False,True,False,False,High,MediaTek Helio G80,4GB,64GB,5020
STK-L21,Huawei,Y6 2019,2019,9.0 (Pie),False,True,False,True,True,False,High,MediaTek MT6761 Helio A22,2GB,32GB,3020
ATU-L31,Huawei,Y6 Prime 2018,2018,8.0 (Oreo),False,True,False,True,True,False,High,Qualcomm Snapdragon 425,3GB,32GB,3000
SM-A105F,Samsung,Galaxy A10,2019,9.0 (Pie),False,True,True,True,True,False,Moderate,Exynos 7884,2GB,32GB,3400
V2027,Vivo,Y70,2020,10.5,True,True,True,True,True,False,High,Qualcomm Snapdragon 665,8GB,128GB,4100
Infinix X6525B,Infinix,Smart 7,2023,12 (Go edition),True,True,False,True,False,False,High,Unisoc SC9863A,2GB,64GB,5000
RMX3830,Realme,Narzo N55 (4GB RAM),2023,13,True,True,False,True,True,False,High,MediaTek Helio G88,4GB,64GB,5000
CPH1931,Oppo,A52,2020,10,True,True,False,True,True,False,High,Qualcomm Snapdragon 665,4GB,64GB,5000
21121119SG,Xiaomi,Xiaomi 12 Pro Dimensity,2022,12,True,True,True,True,True,False,High,MediaTek Dimensity 9000+,8GB,256GB,5160
2201117SG,Xiaomi,Xiaomi 12S Pro,2022,12,True,True,True,True,True,False,High,Qualcomm Snapdragon 8+ Gen 1,8GB,128GB,4600
23108RN04Y,Xiaomi,Redmi Note 13 4G,2024,13,False,True,False,True,True,False,High,Qualcomm Snapdragon 685,6GB,128GB,5000
ALE-L21,Huawei,P8 Lite,2015,5.0 (Lollipop),False,True,True,True,True,False,High,Hisilicon Kirin 620,2GB,16GB,2200
CPH2061,Oppo,A92s,2020,10,True,True,True,True,True,False,High,MediaTek Dimensity 800,8GB,128GB,4000
CPH2113,Oppo,A15,2020,10.0,False,True,False,True,True,False,High,MediaTek Helio P35,2GB,32GB,4230
CPH2179,Oppo,A95 4G,2021,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 662,8GB,128GB,5000
CPH2269,Oppo,Reno7 Z 5G,2022,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 695 5G,8GB,128GB,4500
Infinix X6511B,Infinix,Smart 6,2021,11 (Go edition),False,True,False,True,False,False,High,Unisoc SC9863A,2GB,32GB,5000
Infinix X669D,Infinix,Hot 12 Play NFC,2022,12,True,True,False,True,False,False,High,Unisoc Tiger T610,4GB,64GB,6000
JSN-L22,Huawei,P Smart 2019,2018,9.0 (Pie),True,True,False,True,True,False,High,Hisilicon Kirin 710,3GB,32GB,3400
M2006C3LG,Xiaomi,Redmi 9C NFC,2020,10,True,True,False,True,False,False,High,MediaTek Helio G35,2GB,32GB,5000
REA-NX9,Realme,GT Master Edition,2021,11,True,True,True,True,True,False,High,Qualcomm Snapdragon 778G 5G,8GB,128GB,4300
RMX1941,Realme,C2,2019,9.0 (Pie),False,True,False,True,False,False,High,MediaTek Helio P22,2GB,16GB,4000
RMX2020,Realme,C12,2020,10,False,True,False,True,True,False,High,MediaTek Helio G35,3GB,32GB,6000
SM-A047F,Samsung,Galaxy A04,2022,12,False,True,False,True,False,False,Moderate,MediaTek Helio P35,3GB,32GB,5000
SM-A145F,Samsung,Galaxy A14 5G,2023,13,True,True,False,True,True,False,Moderate,Exynos 1330,4GB,64GB,5000
SM-M315F,Samsung,Galaxy M31,2020,10,True,True,True,True,True,False,Moderate,Exynos 9611,6GB,64GB,6000
SM-N9700,Samsung,Galaxy Note 10 Lite,2020,10,False,True,True,True,True,False,Moderate,Exynos 9810,6GB,128GB,4500
TECNO BB2,Tecno,Pop 2F,2019,8.1 (Oreo) (Go edition),False,True,False,True,False,False,High,MediaTek MT6580,1GB,8GB,2400
TFY-LX2,Huawei,Y8p,2020,10,True,True,False,True,True,False,High,Hisilicon Kirin 710F,4GB,128GB,4000
V2029,Vivo,Y30,2020,10,False,True,False,True,True,False,High,MediaTek Helio P35,4GB,128GB,5000
V2111-EG,Vivo,Y53s 4G,2021,11,True,True,False,True,True,False,High,Qualcomm Snapdragon 680 4G,8GB,128GB,5000
V2203,Vivo,Y21,2021,11.1,True,True,False,True,True,False,High,MediaTek Helio P35,4GB,64GB,5000
vivo 2015,Vivo,Y12s,2020,10.5,False,True,False,True,True,False,High,MediaTek Helio P35,3GB,32GB,5000
"""
    
    # Read mobile specs into a DataFrame
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