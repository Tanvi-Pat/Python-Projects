#!/usr/bin/env python
# coding: utf-8

# In[250]:


#!pip install sweetviz
#Import Libraries
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import pyodbc
from scipy.spatial import distance
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
import threading
from collections import Counter
import psycopg2
import threading
import sys
try:
    import queue
except ImportError:
    import Queue as queue
    
from threading import Thread
from sklearn.preprocessing import QuantileTransformer
from sklearn.cluster import KMeans
from itertools import product
#import sweetviz as sv


# In[178]:


Postgreserver = 'server1'
Postgredbname = 'db1'
Postgreuser = 'user1'
Postgrepassword= 'password1'


# In[180]:


# connect to SQL server and variables for clustering
gpdbconn=psycopg2.connect(host=Postgreserver, dbname=Postgredbname, user=Postgreuser, password=Postgrepassword)
gpdbcursor = gpdbconn.cursor()
gpdbcursor.execute("""select bank,membershipno,txn_3yr_Accessories,	txn_3yr_Anklets_nd_Nose_Pins,	txn_3yr_Appliances,	txn_3yr_Art_nd_Craft,	txn_3yr_Automobile,	txn_3yr_Automotive_Accessories,	txn_3yr_Baby_Care,	txn_3yr_Backpacks,	txn_3yr_Bags_nd_Luggage,	txn_3yr_Bangles_nd_Bracelets,	txn_3yr_Barbie,	txn_3yr_Bath,	txn_3yr_Bath_nd_Body,	txn_3yr_Beauty_nd_Grooming,	txn_3yr_Bedsheets,	txn_3yr_Bedsheets_nd_Pillow_Covers,	txn_3yr_Bike,	txn_3yr_Blankets_nd_Quilts,	txn_3yr_Books_nd_Ebooks_Store,	txn_3yr_Budget_Phones,	txn_3yr_Cakes,	txn_3yr_Cameras_nd_Accessories,	txn_3yr_Candles_nd_Incenses,	txn_3yr_Car,	txn_3yr_Card_Holders,	txn_3yr_Care,	txn_3yr_Cars_And_Automobiles,	txn_3yr_Cars_nd_Bikes,	txn_3yr_Celebrations,	txn_3yr_Celebrations_Rakhi,	txn_3yr_Chains_nd_Hair_Accessories,	txn_3yr_Charity,	txn_3yr_Children,	txn_3yr_Chocolates,	txn_3yr_Christmas,	txn_3yr_Clocks,	txn_3yr_Clothing_nd_Shoes,	txn_3yr_Clutches_nd_Wallets,	txn_3yr_Coins,	txn_3yr_Computers_nd_Peripherals,	txn_3yr_Cooking_Essentials,	txn_3yr_Crystals,	txn_3yr_Cushion_Covers_nd_Pillow_Covers,	txn_3yr_Decor,	txn_3yr_Devotional,	txn_3yr_Diamond_Gold,	txn_3yr_Digital_Cameras,	txn_3yr_Dining,	txn_3yr_Disability,	txn_3yr_Diwan_Sets_nd_Curtains,	txn_3yr_Dolls,	txn_3yr_Dryfruits,	txn_3yr_Earrings,	txn_3yr_Ebooks,	txn_3yr_Education,	txn_3yr_Elderly,	txn_3yr_Electronics,	txn_3yr_Employment,	txn_3yr_Environment,	txn_3yr_Essentials,	txn_3yr_Fashion,	txn_3yr_Fashion_Accessories,	txn_3yr_Flower_Arrangements,	txn_3yr_Flower_Arrangements_nd_Bouquets,	txn_3yr_Flower_Bouquet,	txn_3yr_Fragrances,	txn_3yr_Fun_nd_Engagement,	txn_3yr_Furnishings,	txn_3yr_Furniture,	txn_3yr_Garden_nd_Outdoors,	txn_3yr_General_Merchandise,	txn_3yr_Genesis_Vouchers,	txn_3yr_Gift_Cards_nd_Vouchers,	txn_3yr_Gifts,	txn_3yr_Gold_Coin,	txn_3yr_Gold_nd_Diamond,	txn_3yr_Gold_Plated,	txn_3yr_Good_Luck_Plants,	txn_3yr_Gym_Sets,	txn_3yr_Hair_Care,	txn_3yr_Hampers,	txn_3yr_Handbags,	txn_3yr_Handbags_nd_Accessories,	txn_3yr_Handicrafts,	txn_3yr_Health,	txn_3yr_Health_nd_Fitness,	txn_3yr_Home,	txn_3yr_Home_Entertainment,	txn_3yr_Home_nd_Kitchen,	txn_3yr_Home_Theaters_nd_Speakers,	txn_3yr_Idols,	txn_3yr_Jewellery,	txn_3yr_Jewellery_Boxes,	txn_3yr_Kindle,	txn_3yr_Kitchen,	txn_3yr_Kitchen_Tools,	txn_3yr_Kits_nd_Hampers,	txn_3yr_Kurtas,	txn_3yr_Kurtis,	txn_3yr_Lamps,	txn_3yr_Laptops_Desktops_nd_Others,	txn_3yr_Learning_nd_Education,	txn_3yr_Lifestyle,	txn_3yr_Luxury_Gift_Cards,	txn_3yr_Men,	txn_3yr_MenSGrooming,	txn_3yr_Metal_Alloy,	txn_3yr_Mithai,	txn_3yr_Mobiles,	txn_3yr_Mothers_Day__Celebration,	txn_3yr_Mugs,	txn_3yr_Mugs_nd_Bottles,	txn_3yr_Necklaces,	txn_3yr_Non_Fiction,	txn_3yr_Oberoi_Vouchers,	txn_3yr_Office,	txn_3yr_Organic_Food_nd_Groceries,	txn_3yr_Pearls,	txn_3yr_Pendants,	txn_3yr_Pens,	txn_3yr_Perfumes_nd_Deos,	txn_3yr_Personal_Care,	txn_3yr_Personalized_Cushion,	txn_3yr_Photo_Frames,	txn_3yr_Pillows_nd_Cushion_Covers,	txn_3yr_Points_Donation,	txn_3yr_Print,	txn_3yr_Printers,	txn_3yr_Puja,	txn_3yr_Pu_Leather,	txn_3yr_Restaurants,	txn_3yr_Rings,	txn_3yr_School_Bags,	txn_3yr_Senior_Citizen_Phones,	txn_3yr_Sets,	txn_3yr_Shawls,	txn_3yr_Shoes,	txn_3yr_Silver_Coin,	txn_3yr_Silver_nd_Gold_Plated,	txn_3yr_Silver_Plated,	txn_3yr_Sling_Bags,	txn_3yr_Soft_Toys,	txn_3yr_Sports_Games_nd_Toys,	txn_3yr_Sports_nd_Outdoors,	txn_3yr_State_Bank_Gift_Card,	txn_3yr_Stationery,	txn_3yr_Stoles,	txn_3yr_Storage_nd_Containers,	txn_3yr_Storage_Devices,	txn_3yr_Strolleys,	txn_3yr_Sunglasses,	txn_3yr_Tableware,	txn_3yr_Thalis_nd_Diyas,	txn_3yr_Thread,	txn_3yr_Toys,	txn_3yr_Travel,	txn_3yr_Travel_nd_Hotels,	txn_3yr_Trouser,	txn_3yr_T_Shirts,	txn_3yr_Tvs,	txn_3yr_Utilities,	txn_3yr_Value_Phones,	txn_3yr_Wallets,	txn_3yr_Watches,	txn_3yr_Western_Wear,	txn_3yr_Women,	txn_3yr_Youth,
txn_1yr_Accessories,	txn_1yr_Anklets_nd_Nose_Pins,	txn_1yr_Appliances,	txn_1yr_Art_nd_Craft,	txn_1yr_Automobile,	txn_1yr_Automotive_Accessories,	txn_1yr_Baby_Care,	txn_1yr_Backpacks,	txn_1yr_Bags_nd_Luggage,	txn_1yr_Bangles_nd_Bracelets,	txn_1yr_Barbie,	txn_1yr_Bath,	txn_1yr_Bath_nd_Body,	txn_1yr_Beauty_nd_Grooming,	txn_1yr_Bedsheets,	txn_1yr_Bedsheets_nd_Pillow_Covers,	txn_1yr_Bike,	txn_1yr_Blankets_nd_Quilts,	txn_1yr_Books_nd_Ebooks_Store,	txn_1yr_Budget_Phones,	txn_1yr_Cakes,	txn_1yr_Cameras_nd_Accessories,	txn_1yr_Candles_nd_Incenses,	txn_1yr_Car,	txn_1yr_Card_Holders,	txn_1yr_Care,	txn_1yr_Cars_And_Automobiles,	txn_1yr_Cars_nd_Bikes,	txn_1yr_Celebrations,	txn_1yr_Celebrations_Rakhi,	txn_1yr_Chains_nd_Hair_Accessories,	txn_1yr_Charity,	txn_1yr_Children,	txn_1yr_Chocolates,	txn_1yr_Christmas,	txn_1yr_Clocks,	txn_1yr_Clothing_nd_Shoes,	txn_1yr_Clutches_nd_Wallets,	txn_1yr_Coins,	txn_1yr_Computers_nd_Peripherals,	txn_1yr_Cooking_Essentials,	txn_1yr_Crystals,	txn_1yr_Cushion_Covers_nd_Pillow_Covers,	txn_1yr_Decor,	txn_1yr_Devotional,	txn_1yr_Diamond_Gold,	txn_1yr_Digital_Cameras,	txn_1yr_Dining,	txn_1yr_Disability,	txn_1yr_Diwan_Sets_nd_Curtains,	txn_1yr_Dolls,	txn_1yr_Dryfruits,	txn_1yr_Earrings,	txn_1yr_Ebooks,	txn_1yr_Education,	txn_1yr_Elderly,	txn_1yr_Electronics,	txn_1yr_Employment,	txn_1yr_Environment,	txn_1yr_Essentials,	txn_1yr_Fashion,	txn_1yr_Fashion_Accessories,	txn_1yr_Flower_Arrangements,	txn_1yr_Flower_Arrangements_nd_Bouquets,	txn_1yr_Flower_Bouquet,	txn_1yr_Fragrances,	txn_1yr_Fun_nd_Engagement,	txn_1yr_Furnishings,	txn_1yr_Furniture,	txn_1yr_Garden_nd_Outdoors,	txn_1yr_General_Merchandise,	txn_1yr_Genesis_Vouchers,	txn_1yr_Gift_Cards_nd_Vouchers,	txn_1yr_Gifts,	txn_1yr_Gold_Coin,	txn_1yr_Gold_nd_Diamond,	txn_1yr_Gold_Plated,	txn_1yr_Good_Luck_Plants,	txn_1yr_Gym_Sets,	txn_1yr_Hair_Care,	txn_1yr_Hampers,	txn_1yr_Handbags,	txn_1yr_Handbags_nd_Accessories,	txn_1yr_Handicrafts,	txn_1yr_Health,	txn_1yr_Health_nd_Fitness,	txn_1yr_Home,	txn_1yr_Home_Entertainment,	txn_1yr_Home_nd_Kitchen,	txn_1yr_Home_Theaters_nd_Speakers,	txn_1yr_Idols,	txn_1yr_Jewellery,	txn_1yr_Jewellery_Boxes,	txn_1yr_Kindle,	txn_1yr_Kitchen,	txn_1yr_Kitchen_Tools,	txn_1yr_Kits_nd_Hampers,	txn_1yr_Kurtas,	txn_1yr_Kurtis,	txn_1yr_Lamps,	txn_1yr_Laptops_Desktops_nd_Others,	txn_1yr_Learning_nd_Education,	txn_1yr_Lifestyle,	txn_1yr_Luxury_Gift_Cards,	txn_1yr_Men,	txn_1yr_MenSGrooming,	txn_1yr_Metal_Alloy,	txn_1yr_Mithai,	txn_1yr_Mobiles,	txn_1yr_Mothers_Day__Celebration,	txn_1yr_Mugs,	txn_1yr_Mugs_nd_Bottles,	txn_1yr_Necklaces,	txn_1yr_Non_Fiction,	txn_1yr_Oberoi_Vouchers,	txn_1yr_Office,	txn_1yr_Organic_Food_nd_Groceries,	txn_1yr_Pearls,	txn_1yr_Pendants,	txn_1yr_Pens,	txn_1yr_Perfumes_nd_Deos,	txn_1yr_Personal_Care,	txn_1yr_Personalized_Cushion,	txn_1yr_Photo_Frames,	txn_1yr_Pillows_nd_Cushion_Covers,	txn_1yr_Points_Donation,	txn_1yr_Print,	txn_1yr_Printers,	txn_1yr_Puja,	txn_1yr_Pu_Leather,	txn_1yr_Restaurants,	txn_1yr_Rings,	txn_1yr_School_Bags,	txn_1yr_Senior_Citizen_Phones,	txn_1yr_Sets,	txn_1yr_Shawls,	txn_1yr_Shoes,	txn_1yr_Silver_Coin,	txn_1yr_Silver_nd_Gold_Plated,	txn_1yr_Silver_Plated,	txn_1yr_Sling_Bags,	txn_1yr_Soft_Toys,	txn_1yr_Sports_Games_nd_Toys,	txn_1yr_Sports_nd_Outdoors,	txn_1yr_State_Bank_Gift_Card,	txn_1yr_Stationery,	txn_1yr_Stoles,	txn_1yr_Storage_nd_Containers,	txn_1yr_Storage_Devices,	txn_1yr_Strolleys,	txn_1yr_Sunglasses,	txn_1yr_Tableware,	txn_1yr_Thalis_nd_Diyas,	txn_1yr_Thread,	txn_1yr_Toys,	txn_1yr_Travel,	txn_1yr_Travel_nd_Hotels,	txn_1yr_Trouser,	txn_1yr_T_Shirts,	txn_1yr_Tvs,	txn_1yr_Utilities,	txn_1yr_Value_Phones,	txn_1yr_Wallets,	txn_1yr_Watches,	txn_1yr_Western_Wear,	txn_1yr_Women,	txn_1yr_Youth,monetory,recency,frequency
 from db.merchandise_cluster_variables """)
df = pd.DataFrame(gpdbcursor.fetchall(),columns = ['bank','membershipno','txn_3yr_Accessories','txn_3yr_Anklets_nd_Nose_Pins',	'txn_3yr_Appliances',	'txn_3yr_Art_nd_Craft',	'txn_3yr_Automobile',	'txn_3yr_Automotive_Accessories',	'txn_3yr_Baby_Care',	'txn_3yr_Backpacks',	'txn_3yr_Bags_nd_Luggage',	'txn_3yr_Bangles_nd_Bracelets',	'txn_3yr_Barbie',	'txn_3yr_Bath',	'txn_3yr_Bath_nd_Body',	'txn_3yr_Beauty_nd_Grooming',	'txn_3yr_Bedsheets',	'txn_3yr_Bedsheets_nd_Pillow_Covers',	'txn_3yr_Bike',	'txn_3yr_Blankets_nd_Quilts',	'txn_3yr_Books_nd_Ebooks_Store',	'txn_3yr_Budget_Phones',	'txn_3yr_Cakes',	'txn_3yr_Cameras_nd_Accessories',	'txn_3yr_Candles_nd_Incenses',	'txn_3yr_Car',	'txn_3yr_Card_Holders',	'txn_3yr_Care',	'txn_3yr_Cars_And_Automobiles',	'txn_3yr_Cars_nd_Bikes',	'txn_3yr_Celebrations',	'txn_3yr_Celebrations_Rakhi',	'txn_3yr_Chains_nd_Hair_Accessories',	'txn_3yr_Charity',	'txn_3yr_Children',	'txn_3yr_Chocolates',	'txn_3yr_Christmas',	'txn_3yr_Clocks',	'txn_3yr_Clothing_nd_Shoes',	'txn_3yr_Clutches_nd_Wallets',	'txn_3yr_Coins',	'txn_3yr_Computers_nd_Peripherals',	'txn_3yr_Cooking_Essentials',	'txn_3yr_Crystals',	'txn_3yr_Cushion_Covers_nd_Pillow_Covers',	'txn_3yr_Decor',	'txn_3yr_Devotional',	'txn_3yr_Diamond_Gold',	'txn_3yr_Digital_Cameras',	'txn_3yr_Dining',	'txn_3yr_Disability',	'txn_3yr_Diwan_Sets_nd_Curtains',	'txn_3yr_Dolls',	'txn_3yr_Dryfruits',	'txn_3yr_Earrings',	'txn_3yr_Ebooks',	'txn_3yr_Education',	'txn_3yr_Elderly',	'txn_3yr_Electronics',	'txn_3yr_Employment',	'txn_3yr_Environment',	'txn_3yr_Essentials',	'txn_3yr_Fashion',	'txn_3yr_Fashion_Accessories',	'txn_3yr_Flower_Arrangements',	'txn_3yr_Flower_Arrangements_nd_Bouquets',	'txn_3yr_Flower_Bouquet',	'txn_3yr_Fragrances',	'txn_3yr_Fun_nd_Engagement',	'txn_3yr_Furnishings',	'txn_3yr_Furniture',	'txn_3yr_Garden_nd_Outdoors',	'txn_3yr_General_Merchandise',	'txn_3yr_Genesis_Vouchers',	'txn_3yr_Gift_Cards_nd_Vouchers',	'txn_3yr_Gifts',	'txn_3yr_Gold_Coin',	'txn_3yr_Gold_nd_Diamond',	'txn_3yr_Gold_Plated',	'txn_3yr_Good_Luck_Plants',	'txn_3yr_Gym_Sets',	'txn_3yr_Hair_Care',	'txn_3yr_Hampers',	'txn_3yr_Handbags',	'txn_3yr_Handbags_nd_Accessories',	'txn_3yr_Handicrafts',	'txn_3yr_Health',	'txn_3yr_Health_nd_Fitness',	'txn_3yr_Home',	'txn_3yr_Home_Entertainment',	'txn_3yr_Home_nd_Kitchen',	'txn_3yr_Home_Theaters_nd_Speakers',	'txn_3yr_Idols',	'txn_3yr_Jewellery',	'txn_3yr_Jewellery_Boxes',	'txn_3yr_Kindle',	'txn_3yr_Kitchen',	'txn_3yr_Kitchen_Tools',	'txn_3yr_Kits_nd_Hampers',	'txn_3yr_Kurtas',	'txn_3yr_Kurtis',	'txn_3yr_Lamps',	'txn_3yr_Laptops_Desktops_nd_Others',	'txn_3yr_Learning_nd_Education',	'txn_3yr_Lifestyle',	'txn_3yr_Luxury_Gift_Cards',	'txn_3yr_Men',	'txn_3yr_MenSGrooming',	'txn_3yr_Metal_Alloy',	'txn_3yr_Mithai',	'txn_3yr_Mobiles',	'txn_3yr_Mothers_Day__Celebration',	'txn_3yr_Mugs',	'txn_3yr_Mugs_nd_Bottles',	'txn_3yr_Necklaces',	'txn_3yr_Non_Fiction',	'txn_3yr_Oberoi_Vouchers',	'txn_3yr_Office',	'txn_3yr_Organic_Food_nd_Groceries',	'txn_3yr_Pearls',	'txn_3yr_Pendants',	'txn_3yr_Pens',	'txn_3yr_Perfumes_nd_Deos',	'txn_3yr_Personal_Care',	'txn_3yr_Personalized_Cushion',	'txn_3yr_Photo_Frames',	'txn_3yr_Pillows_nd_Cushion_Covers',	'txn_3yr_Points_Donation',	'txn_3yr_Print',	'txn_3yr_Printers',	'txn_3yr_Puja',	'txn_3yr_Pu_Leather',	'txn_3yr_Restaurants',	'txn_3yr_Rings',	'txn_3yr_School_Bags',	'txn_3yr_Senior_Citizen_Phones',	'txn_3yr_Sets',	'txn_3yr_Shawls',	'txn_3yr_Shoes',	'txn_3yr_Silver_Coin',	'txn_3yr_Silver_nd_Gold_Plated',	'txn_3yr_Silver_Plated',	'txn_3yr_Sling_Bags',	'txn_3yr_Soft_Toys',	'txn_3yr_Sports_Games_nd_Toys',	'txn_3yr_Sports_nd_Outdoors',	'txn_3yr_State_Bank_Gift_Card',	'txn_3yr_Stationery',	'txn_3yr_Stoles',	'txn_3yr_Storage_nd_Containers',	'txn_3yr_Storage_Devices',	'txn_3yr_Strolleys',	'txn_3yr_Sunglasses',	'txn_3yr_Tableware',	'txn_3yr_Thalis_nd_Diyas',	'txn_3yr_Thread',	'txn_3yr_Toys',	'txn_3yr_Travel',	'txn_3yr_Travel_nd_Hotels',	'txn_3yr_Trouser',	'txn_3yr_T_Shirts',	'txn_3yr_Tvs',	'txn_3yr_Utilities',	'txn_3yr_Value_Phones',	'txn_3yr_Wallets',	'txn_3yr_Watches',	'txn_3yr_Western_Wear',	'txn_3yr_Women',	'txn_3yr_Youth',
'txn_1yr_Accessories',	'txn_1yr_Anklets_nd_Nose_Pins',	'txn_1yr_Appliances',	'txn_1yr_Art_nd_Craft',	'txn_1yr_Automobile',	'txn_1yr_Automotive_Accessories',	'txn_1yr_Baby_Care',	'txn_1yr_Backpacks',	'txn_1yr_Bags_nd_Luggage',	'txn_1yr_Bangles_nd_Bracelets',	'txn_1yr_Barbie',	'txn_1yr_Bath',	'txn_1yr_Bath_nd_Body',	'txn_1yr_Beauty_nd_Grooming',	'txn_1yr_Bedsheets',	'txn_1yr_Bedsheets_nd_Pillow_Covers',	'txn_1yr_Bike',	'txn_1yr_Blankets_nd_Quilts',	'txn_1yr_Books_nd_Ebooks_Store',	'txn_1yr_Budget_Phones',	'txn_1yr_Cakes',	'txn_1yr_Cameras_nd_Accessories',	'txn_1yr_Candles_nd_Incenses',	'txn_1yr_Car',	'txn_1yr_Card_Holders',	'txn_1yr_Care',	'txn_1yr_Cars_And_Automobiles',	'txn_1yr_Cars_nd_Bikes',	'txn_1yr_Celebrations',	'txn_1yr_Celebrations_Rakhi',	'txn_1yr_Chains_nd_Hair_Accessories',	'txn_1yr_Charity',	'txn_1yr_Children',	'txn_1yr_Chocolates',	'txn_1yr_Christmas',	'txn_1yr_Clocks',	'txn_1yr_Clothing_nd_Shoes',	'txn_1yr_Clutches_nd_Wallets',	'txn_1yr_Coins',	'txn_1yr_Computers_nd_Peripherals',	'txn_1yr_Cooking_Essentials',	'txn_1yr_Crystals',	'txn_1yr_Cushion_Covers_nd_Pillow_Covers',	'txn_1yr_Decor',	'txn_1yr_Devotional',	'txn_1yr_Diamond_Gold',	'txn_1yr_Digital_Cameras',	'txn_1yr_Dining',	'txn_1yr_Disability',	'txn_1yr_Diwan_Sets_nd_Curtains',	'txn_1yr_Dolls',	'txn_1yr_Dryfruits',	'txn_1yr_Earrings',	'txn_1yr_Ebooks',	'txn_1yr_Education',	'txn_1yr_Elderly',	'txn_1yr_Electronics',	'txn_1yr_Employment',	'txn_1yr_Environment',	'txn_1yr_Essentials',	'txn_1yr_Fashion',	'txn_1yr_Fashion_Accessories',	'txn_1yr_Flower_Arrangements',	'txn_1yr_Flower_Arrangements_nd_Bouquets',	'txn_1yr_Flower_Bouquet',	'txn_1yr_Fragrances',	'txn_1yr_Fun_nd_Engagement',	'txn_1yr_Furnishings',	'txn_1yr_Furniture',	'txn_1yr_Garden_nd_Outdoors',	'txn_1yr_General_Merchandise',	'txn_1yr_Genesis_Vouchers',	'txn_1yr_Gift_Cards_nd_Vouchers',	'txn_1yr_Gifts',	'txn_1yr_Gold_Coin',	'txn_1yr_Gold_nd_Diamond',	'txn_1yr_Gold_Plated',	'txn_1yr_Good_Luck_Plants',	'txn_1yr_Gym_Sets',	'txn_1yr_Hair_Care',	'txn_1yr_Hampers',	'txn_1yr_Handbags',	'txn_1yr_Handbags_nd_Accessories',	'txn_1yr_Handicrafts',	'txn_1yr_Health',	'txn_1yr_Health_nd_Fitness',	'txn_1yr_Home',	'txn_1yr_Home_Entertainment',	'txn_1yr_Home_nd_Kitchen',	'txn_1yr_Home_Theaters_nd_Speakers',	'txn_1yr_Idols',	'txn_1yr_Jewellery',	'txn_1yr_Jewellery_Boxes',	'txn_1yr_Kindle',	'txn_1yr_Kitchen',	'txn_1yr_Kitchen_Tools',	'txn_1yr_Kits_nd_Hampers',	'txn_1yr_Kurtas',	'txn_1yr_Kurtis',	'txn_1yr_Lamps',	'txn_1yr_Laptops_Desktops_nd_Others',	'txn_1yr_Learning_nd_Education',	'txn_1yr_Lifestyle',	'txn_1yr_Luxury_Gift_Cards',	'txn_1yr_Men',	'txn_1yr_MenGrooming',	'txn_1yr_Metal_Alloy',	'txn_1yr_Mithai',	'txn_1yr_Mobiles',	'txn_1yr_Mothers_Day__Celebration',	'txn_1yr_Mugs',	'txn_1yr_Mugs_nd_Bottles',	'txn_1yr_Necklaces',	'txn_1yr_Non_Fiction',	'txn_1yr_Oberoi_Vouchers',	'txn_1yr_Office',	'txn_1yr_Organic_Food_nd_Groceries',	'txn_1yr_Pearls',	'txn_1yr_Pendants',	'txn_1yr_Pens',	'txn_1yr_Perfumes_nd_Deos',	'txn_1yr_Personal_Care',	'txn_1yr_Personalized_Cushion',	'txn_1yr_Photo_Frames',	'txn_1yr_Pillows_nd_Cushion_Covers',	'txn_1yr_Points_Donation',	'txn_1yr_Print',	'txn_1yr_Printers',	'txn_1yr_Puja',	'txn_1yr_Pu_Leather',	'txn_1yr_Restaurants',	'txn_1yr_Rings',	'txn_1yr_School_Bags',	'txn_1yr_Senior_Citizen_Phones',	'txn_1yr_Sets',	'txn_1yr_Shawls',	'txn_1yr_Shoes',	'txn_1yr_Silver_Coin',	'txn_1yr_Silver_nd_Gold_Plated',	'txn_1yr_Silver_Plated',	'txn_1yr_Sling_Bags',	'txn_1yr_Soft_Toys',	'txn_1yr_Sports_Games_nd_Toys',	'txn_1yr_Sports_nd_Outdoors',	'txn_1yr_State_Bank_Gift_Card',	'txn_1yr_Stationery',	'txn_1yr_Stoles',	'txn_1yr_Storage_nd_Containers',	'txn_1yr_Storage_Devices',	'txn_1yr_Strolleys',	'txn_1yr_Sunglasses',	'txn_1yr_Tableware',	'txn_1yr_Thalis_nd_Diyas',	'txn_1yr_Thread',	'txn_1yr_Toys',	'txn_1yr_Travel',	'txn_1yr_Travel_nd_Hotels',	'txn_1yr_Trouser',	'txn_1yr_T_Shirts',	'txn_1yr_Tvs',	'txn_1yr_Utilities',	'txn_1yr_Value_Phones',	'txn_1yr_Wallets',	'txn_1yr_Watches',	'txn_1yr_Western_Wear',	'txn_1yr_Women',	'txn_1yr_Youth','monetory','recency','frequency'])
gpdbconn.close()
print(len(df))


# In[181]:


df[['txn_3yr_Accessories','txn_3yr_Anklets_nd_Nose_Pins',	'txn_3yr_Appliances',	'txn_3yr_Art_nd_Craft',	'txn_3yr_Automobile',	'txn_3yr_Automotive_Accessories',	'txn_3yr_Baby_Care',	'txn_3yr_Backpacks',	'txn_3yr_Bags_nd_Luggage',	'txn_3yr_Bangles_nd_Bracelets',	'txn_3yr_Barbie',	'txn_3yr_Bath',	'txn_3yr_Bath_nd_Body',	'txn_3yr_Beauty_nd_Grooming',	'txn_3yr_Bedsheets',	'txn_3yr_Bedsheets_nd_Pillow_Covers',	'txn_3yr_Bike',	'txn_3yr_Blankets_nd_Quilts',	'txn_3yr_Books_nd_Ebooks_Store',	'txn_3yr_Budget_Phones',	'txn_3yr_Cakes',	'txn_3yr_Cameras_nd_Accessories',	'txn_3yr_Candles_nd_Incenses',	'txn_3yr_Car',	'txn_3yr_Card_Holders',	'txn_3yr_Care',	'txn_3yr_Cars_And_Automobiles',	'txn_3yr_Cars_nd_Bikes',	'txn_3yr_Celebrations',	'txn_3yr_Celebrations_Rakhi',	'txn_3yr_Chains_nd_Hair_Accessories',	'txn_3yr_Charity',	'txn_3yr_Children',	'txn_3yr_Chocolates',	'txn_3yr_Christmas',	'txn_3yr_Clocks',	'txn_3yr_Clothing_nd_Shoes',	'txn_3yr_Clutches_nd_Wallets',	'txn_3yr_Coins',	'txn_3yr_Computers_nd_Peripherals',	'txn_3yr_Cooking_Essentials',	'txn_3yr_Crystals',	'txn_3yr_Cushion_Covers_nd_Pillow_Covers',	'txn_3yr_Decor',	'txn_3yr_Devotional',	'txn_3yr_Diamond_Gold',	'txn_3yr_Digital_Cameras',	'txn_3yr_Dining',	'txn_3yr_Disability',	'txn_3yr_Diwan_Sets_nd_Curtains',	'txn_3yr_Dolls',	'txn_3yr_Dryfruits',	'txn_3yr_Earrings',	'txn_3yr_Ebooks',	'txn_3yr_Education',	'txn_3yr_Elderly',	'txn_3yr_Electronics',	'txn_3yr_Employment',	'txn_3yr_Environment',	'txn_3yr_Essentials',	'txn_3yr_Fashion',	'txn_3yr_Fashion_Accessories',	'txn_3yr_Flower_Arrangements',	'txn_3yr_Flower_Arrangements_nd_Bouquets',	'txn_3yr_Flower_Bouquet',	'txn_3yr_Fragrances',	'txn_3yr_Fun_nd_Engagement',	'txn_3yr_Furnishings',	'txn_3yr_Furniture',	'txn_3yr_Garden_nd_Outdoors',	'txn_3yr_General_Merchandise',	'txn_3yr_Genesis_Vouchers',	'txn_3yr_Gift_Cards_nd_Vouchers',	'txn_3yr_Gifts',	'txn_3yr_Gold_Coin',	'txn_3yr_Gold_nd_Diamond',	'txn_3yr_Gold_Plated',	'txn_3yr_Good_Luck_Plants',	'txn_3yr_Gym_Sets',	'txn_3yr_Hair_Care',	'txn_3yr_Hampers',	'txn_3yr_Handbags',	'txn_3yr_Handbags_nd_Accessories',	'txn_3yr_Handicrafts',	'txn_3yr_Health',	'txn_3yr_Health_nd_Fitness',	'txn_3yr_Home',	'txn_3yr_Home_Entertainment',	'txn_3yr_Home_nd_Kitchen',	'txn_3yr_Home_Theaters_nd_Speakers',	'txn_3yr_Idols',	'txn_3yr_Jewellery',	'txn_3yr_Jewellery_Boxes',	'txn_3yr_Kindle',	'txn_3yr_Kitchen',	'txn_3yr_Kitchen_Tools',	'txn_3yr_Kits_nd_Hampers',	'txn_3yr_Kurtas',	'txn_3yr_Kurtis',	'txn_3yr_Lamps',	'txn_3yr_Laptops_Desktops_nd_Others',	'txn_3yr_Learning_nd_Education',	'txn_3yr_Lifestyle',	'txn_3yr_Luxury_Gift_Cards',	'txn_3yr_Men',	'txn_3yr_MenSGrooming',	'txn_3yr_Metal_Alloy',	'txn_3yr_Mithai',	'txn_3yr_Mobiles',	'txn_3yr_Mothers_Day__Celebration',	'txn_3yr_Mugs',	'txn_3yr_Mugs_nd_Bottles',	'txn_3yr_Necklaces',	'txn_3yr_Non_Fiction',	'txn_3yr_Oberoi_Vouchers',	'txn_3yr_Office',	'txn_3yr_Organic_Food_nd_Groceries',	'txn_3yr_Pearls',	'txn_3yr_Pendants',	'txn_3yr_Pens',	'txn_3yr_Perfumes_nd_Deos',	'txn_3yr_Personal_Care',	'txn_3yr_Personalized_Cushion',	'txn_3yr_Photo_Frames',	'txn_3yr_Pillows_nd_Cushion_Covers',	'txn_3yr_Points_Donation',	'txn_3yr_Print',	'txn_3yr_Printers',	'txn_3yr_Puja',	'txn_3yr_Pu_Leather',	'txn_3yr_Restaurants',	'txn_3yr_Rings',	'txn_3yr_School_Bags',	'txn_3yr_Senior_Citizen_Phones',	'txn_3yr_Sets',	'txn_3yr_Shawls',	'txn_3yr_Shoes',	'txn_3yr_Silver_Coin',	'txn_3yr_Silver_nd_Gold_Plated',	'txn_3yr_Silver_Plated',	'txn_3yr_Sling_Bags',	'txn_3yr_Soft_Toys',	'txn_3yr_Sports_Games_nd_Toys',	'txn_3yr_Sports_nd_Outdoors',	'txn_3yr_State_Bank_Gift_Card',	'txn_3yr_Stationery',	'txn_3yr_Stoles',	'txn_3yr_Storage_nd_Containers',	'txn_3yr_Storage_Devices',	'txn_3yr_Strolleys',	'txn_3yr_Sunglasses',	'txn_3yr_Tableware',	'txn_3yr_Thalis_nd_Diyas',	'txn_3yr_Thread',	'txn_3yr_Toys',	'txn_3yr_Travel',	'txn_3yr_Travel_nd_Hotels',	'txn_3yr_Trouser',	'txn_3yr_T_Shirts',	'txn_3yr_Tvs',	'txn_3yr_Utilities',	'txn_3yr_Value_Phones',	'txn_3yr_Wallets',	'txn_3yr_Watches',	'txn_3yr_Western_Wear',	'txn_3yr_Women',	'txn_3yr_Youth',
'txn_1yr_Accessories',	'txn_1yr_Anklets_nd_Nose_Pins',	'txn_1yr_Appliances',	'txn_1yr_Art_nd_Craft',	'txn_1yr_Automobile','txn_1yr_Automotive_Accessories',	'txn_1yr_Baby_Care',	'txn_1yr_Backpacks',	'txn_1yr_Bags_nd_Luggage',	'txn_1yr_Bangles_nd_Bracelets',	'txn_1yr_Barbie',	'txn_1yr_Bath',	'txn_1yr_Bath_nd_Body',	'txn_1yr_Beauty_nd_Grooming',	'txn_1yr_Bedsheets',	'txn_1yr_Bedsheets_nd_Pillow_Covers',	'txn_1yr_Bike',	'txn_1yr_Blankets_nd_Quilts',	'txn_1yr_Books_nd_Ebooks_Store',	'txn_1yr_Budget_Phones',	'txn_1yr_Cakes',	'txn_1yr_Cameras_nd_Accessories',	'txn_1yr_Candles_nd_Incenses',	'txn_1yr_Car',	'txn_1yr_Card_Holders',	'txn_1yr_Care',	'txn_1yr_Cars_And_Automobiles',	'txn_1yr_Cars_nd_Bikes',	'txn_1yr_Celebrations',	'txn_1yr_Celebrations_Rakhi',	'txn_1yr_Chains_nd_Hair_Accessories',	'txn_1yr_Charity',	'txn_1yr_Children',	'txn_1yr_Chocolates',	'txn_1yr_Christmas',	'txn_1yr_Clocks',	'txn_1yr_Clothing_nd_Shoes',	'txn_1yr_Clutches_nd_Wallets',	'txn_1yr_Coins',	'txn_1yr_Computers_nd_Peripherals',	'txn_1yr_Cooking_Essentials',	'txn_1yr_Crystals',	'txn_1yr_Cushion_Covers_nd_Pillow_Covers',	'txn_1yr_Decor',	'txn_1yr_Devotional',	'txn_1yr_Diamond_Gold',	'txn_1yr_Digital_Cameras',	'txn_1yr_Dining',	'txn_1yr_Disability',	'txn_1yr_Diwan_Sets_nd_Curtains',	'txn_1yr_Dolls',	'txn_1yr_Dryfruits',	'txn_1yr_Earrings',	'txn_1yr_Ebooks',	'txn_1yr_Education',	'txn_1yr_Elderly',	'txn_1yr_Electronics',	'txn_1yr_Employment',	'txn_1yr_Environment',	'txn_1yr_Essentials',	'txn_1yr_Fashion',	'txn_1yr_Fashion_Accessories',	'txn_1yr_Flower_Arrangements',	'txn_1yr_Flower_Arrangements_nd_Bouquets',	'txn_1yr_Flower_Bouquet',	'txn_1yr_Fragrances',	'txn_1yr_Fun_nd_Engagement',	'txn_1yr_Furnishings',	'txn_1yr_Furniture',	'txn_1yr_Garden_nd_Outdoors',	'txn_1yr_General_Merchandise',	'txn_1yr_Genesis_Vouchers',	'txn_1yr_Gift_Cards_nd_Vouchers',	'txn_1yr_Gifts',	'txn_1yr_Gold_Coin',	'txn_1yr_Gold_nd_Diamond',	'txn_1yr_Gold_Plated',	'txn_1yr_Good_Luck_Plants',	'txn_1yr_Gym_Sets',	'txn_1yr_Hair_Care',	'txn_1yr_Hampers',	'txn_1yr_Handbags',	'txn_1yr_Handbags_nd_Accessories',	'txn_1yr_Handicrafts',	'txn_1yr_Health',	'txn_1yr_Health_nd_Fitness',	'txn_1yr_Home',	'txn_1yr_Home_Entertainment',	'txn_1yr_Home_nd_Kitchen',	'txn_1yr_Home_Theaters_nd_Speakers',	'txn_1yr_Idols',	'txn_1yr_Jewellery',	'txn_1yr_Jewellery_Boxes',	'txn_1yr_Kindle',	'txn_1yr_Kitchen',	'txn_1yr_Kitchen_Tools',	'txn_1yr_Kits_nd_Hampers',	'txn_1yr_Kurtas',	'txn_1yr_Kurtis',	'txn_1yr_Lamps',	'txn_1yr_Laptops_Desktops_nd_Others',	'txn_1yr_Learning_nd_Education',	'txn_1yr_Lifestyle',	'txn_1yr_Luxury_Gift_Cards',	'txn_1yr_Men',	'txn_1yr_MenGrooming',	'txn_1yr_Metal_Alloy',	'txn_1yr_Mithai',	'txn_1yr_Mobiles',	'txn_1yr_Mothers_Day__Celebration',	'txn_1yr_Mugs',	'txn_1yr_Mugs_nd_Bottles',	'txn_1yr_Necklaces',	'txn_1yr_Non_Fiction',	'txn_1yr_Oberoi_Vouchers',	'txn_1yr_Office',	'txn_1yr_Organic_Food_nd_Groceries',	'txn_1yr_Pearls',	'txn_1yr_Pendants',	'txn_1yr_Pens',	'txn_1yr_Perfumes_nd_Deos',	'txn_1yr_Personal_Care',	'txn_1yr_Personalized_Cushion',	'txn_1yr_Photo_Frames',	'txn_1yr_Pillows_nd_Cushion_Covers',	'txn_1yr_Points_Donation',	'txn_1yr_Print',	'txn_1yr_Printers',	'txn_1yr_Puja',	'txn_1yr_Pu_Leather',	'txn_1yr_Restaurants',	'txn_1yr_Rings',	'txn_1yr_School_Bags',	'txn_1yr_Senior_Citizen_Phones',	'txn_1yr_Sets',	'txn_1yr_Shawls',	'txn_1yr_Shoes',	'txn_1yr_Silver_Coin',	'txn_1yr_Silver_nd_Gold_Plated',	'txn_1yr_Silver_Plated',	'txn_1yr_Sling_Bags',	'txn_1yr_Soft_Toys',	'txn_1yr_Sports_Games_nd_Toys',	'txn_1yr_Sports_nd_Outdoors',	'txn_1yr_State_Bank_Gift_Card',	'txn_1yr_Stationery',	'txn_1yr_Stoles',	'txn_1yr_Storage_nd_Containers',	'txn_1yr_Storage_Devices',	'txn_1yr_Strolleys',	'txn_1yr_Sunglasses',	'txn_1yr_Tableware',	'txn_1yr_Thalis_nd_Diyas',	'txn_1yr_Thread',	'txn_1yr_Toys',	'txn_1yr_Travel',	'txn_1yr_Travel_nd_Hotels',	'txn_1yr_Trouser',	'txn_1yr_T_Shirts',	'txn_1yr_Tvs',	'txn_1yr_Utilities',	'txn_1yr_Value_Phones',	'txn_1yr_Wallets',	'txn_1yr_Watches',	'txn_1yr_Western_Wear',	'txn_1yr_Women',	'txn_1yr_Youth',
'monetory','recency','frequency']] = df[['txn_3yr_Accessories','txn_3yr_Anklets_nd_Nose_Pins',	'txn_3yr_Appliances',	'txn_3yr_Art_nd_Craft',	'txn_3yr_Automobile',	'txn_3yr_Automotive_Accessories',	'txn_3yr_Baby_Care',	'txn_3yr_Backpacks',	'txn_3yr_Bags_nd_Luggage',	'txn_3yr_Bangles_nd_Bracelets',	'txn_3yr_Barbie',	'txn_3yr_Bath',	'txn_3yr_Bath_nd_Body',	'txn_3yr_Beauty_nd_Grooming',	'txn_3yr_Bedsheets',	'txn_3yr_Bedsheets_nd_Pillow_Covers',	'txn_3yr_Bike',	'txn_3yr_Blankets_nd_Quilts',	'txn_3yr_Books_nd_Ebooks_Store',	'txn_3yr_Budget_Phones',	'txn_3yr_Cakes',	'txn_3yr_Cameras_nd_Accessories',	'txn_3yr_Candles_nd_Incenses',	'txn_3yr_Car',	'txn_3yr_Card_Holders',	'txn_3yr_Care',	'txn_3yr_Cars_And_Automobiles',	'txn_3yr_Cars_nd_Bikes',	'txn_3yr_Celebrations',	'txn_3yr_Celebrations_Rakhi',	'txn_3yr_Chains_nd_Hair_Accessories',	'txn_3yr_Charity',	'txn_3yr_Children',	'txn_3yr_Chocolates',	'txn_3yr_Christmas',	'txn_3yr_Clocks',	'txn_3yr_Clothing_nd_Shoes',	'txn_3yr_Clutches_nd_Wallets',	'txn_3yr_Coins',	'txn_3yr_Computers_nd_Peripherals',	'txn_3yr_Cooking_Essentials',	'txn_3yr_Crystals',	'txn_3yr_Cushion_Covers_nd_Pillow_Covers',	'txn_3yr_Decor',	'txn_3yr_Devotional',	'txn_3yr_Diamond_Gold',	'txn_3yr_Digital_Cameras',	'txn_3yr_Dining',	'txn_3yr_Disability',	'txn_3yr_Diwan_Sets_nd_Curtains',	'txn_3yr_Dolls',	'txn_3yr_Dryfruits',	'txn_3yr_Earrings',	'txn_3yr_Ebooks',	'txn_3yr_Education',	'txn_3yr_Elderly',	'txn_3yr_Electronics',	'txn_3yr_Employment',	'txn_3yr_Environment',	'txn_3yr_Essentials',	'txn_3yr_Fashion',	'txn_3yr_Fashion_Accessories',	'txn_3yr_Flower_Arrangements',	'txn_3yr_Flower_Arrangements_nd_Bouquets',	'txn_3yr_Flower_Bouquet',	'txn_3yr_Fragrances',	'txn_3yr_Fun_nd_Engagement',	'txn_3yr_Furnishings',	'txn_3yr_Furniture',	'txn_3yr_Garden_nd_Outdoors',	'txn_3yr_General_Merchandise',	'txn_3yr_Genesis_Vouchers',	'txn_3yr_Gift_Cards_nd_Vouchers',	'txn_3yr_Gifts',	'txn_3yr_Gold_Coin',	'txn_3yr_Gold_nd_Diamond',	'txn_3yr_Gold_Plated',	'txn_3yr_Good_Luck_Plants',	'txn_3yr_Gym_Sets',	'txn_3yr_Hair_Care',	'txn_3yr_Hampers',	'txn_3yr_Handbags',	'txn_3yr_Handbags_nd_Accessories',	'txn_3yr_Handicrafts',	'txn_3yr_Health',	'txn_3yr_Health_nd_Fitness',	'txn_3yr_Home',	'txn_3yr_Home_Entertainment',	'txn_3yr_Home_nd_Kitchen',	'txn_3yr_Home_Theaters_nd_Speakers',	'txn_3yr_Idols',	'txn_3yr_Jewellery',	'txn_3yr_Jewellery_Boxes',	'txn_3yr_Kindle',	'txn_3yr_Kitchen',	'txn_3yr_Kitchen_Tools',	'txn_3yr_Kits_nd_Hampers',	'txn_3yr_Kurtas',	'txn_3yr_Kurtis',	'txn_3yr_Lamps',	'txn_3yr_Laptops_Desktops_nd_Others',	'txn_3yr_Learning_nd_Education',	'txn_3yr_Lifestyle',	'txn_3yr_Luxury_Gift_Cards',	'txn_3yr_Men',	'txn_3yr_MenSGrooming',	'txn_3yr_Metal_Alloy',	'txn_3yr_Mithai',	'txn_3yr_Mobiles',	'txn_3yr_Mothers_Day__Celebration',	'txn_3yr_Mugs',	'txn_3yr_Mugs_nd_Bottles',	'txn_3yr_Necklaces',	'txn_3yr_Non_Fiction',	'txn_3yr_Oberoi_Vouchers',	'txn_3yr_Office',	'txn_3yr_Organic_Food_nd_Groceries',	'txn_3yr_Pearls',	'txn_3yr_Pendants',	'txn_3yr_Pens',	'txn_3yr_Perfumes_nd_Deos',	'txn_3yr_Personal_Care',	'txn_3yr_Personalized_Cushion',	'txn_3yr_Photo_Frames',	'txn_3yr_Pillows_nd_Cushion_Covers',	'txn_3yr_Points_Donation',	'txn_3yr_Print',	'txn_3yr_Printers',	'txn_3yr_Puja',	'txn_3yr_Pu_Leather',	'txn_3yr_Restaurants',	'txn_3yr_Rings',	'txn_3yr_School_Bags',	'txn_3yr_Senior_Citizen_Phones',	'txn_3yr_Sets',	'txn_3yr_Shawls',	'txn_3yr_Shoes',	'txn_3yr_Silver_Coin',	'txn_3yr_Silver_nd_Gold_Plated',	'txn_3yr_Silver_Plated',	'txn_3yr_Sling_Bags',	'txn_3yr_Soft_Toys',	'txn_3yr_Sports_Games_nd_Toys',	'txn_3yr_Sports_nd_Outdoors',	'txn_3yr_State_Bank_Gift_Card',	'txn_3yr_Stationery',	'txn_3yr_Stoles',	'txn_3yr_Storage_nd_Containers',	'txn_3yr_Storage_Devices',	'txn_3yr_Strolleys',	'txn_3yr_Sunglasses',	'txn_3yr_Tableware',	'txn_3yr_Thalis_nd_Diyas',	'txn_3yr_Thread',	'txn_3yr_Toys',	'txn_3yr_Travel',	'txn_3yr_Travel_nd_Hotels',	'txn_3yr_Trouser',	'txn_3yr_T_Shirts',	'txn_3yr_Tvs',	'txn_3yr_Utilities',	'txn_3yr_Value_Phones',	'txn_3yr_Wallets',	'txn_3yr_Watches',	'txn_3yr_Western_Wear',	'txn_3yr_Women',	'txn_3yr_Youth',
'txn_1yr_Accessories',	'txn_1yr_Anklets_nd_Nose_Pins',	'txn_1yr_Appliances',	'txn_1yr_Art_nd_Craft',	'txn_1yr_Automobile',	'txn_1yr_Automotive_Accessories',	'txn_1yr_Baby_Care',	'txn_1yr_Backpacks',	'txn_1yr_Bags_nd_Luggage',	'txn_1yr_Bangles_nd_Bracelets',	'txn_1yr_Barbie',	'txn_1yr_Bath',	'txn_1yr_Bath_nd_Body',	'txn_1yr_Beauty_nd_Grooming',	'txn_1yr_Bedsheets',	'txn_1yr_Bedsheets_nd_Pillow_Covers',	'txn_1yr_Bike',	'txn_1yr_Blankets_nd_Quilts',	'txn_1yr_Books_nd_Ebooks_Store',	'txn_1yr_Budget_Phones',	'txn_1yr_Cakes',	'txn_1yr_Cameras_nd_Accessories',	'txn_1yr_Candles_nd_Incenses',	'txn_1yr_Car',	'txn_1yr_Card_Holders',	'txn_1yr_Care',	'txn_1yr_Cars_And_Automobiles',	'txn_1yr_Cars_nd_Bikes',	'txn_1yr_Celebrations',	'txn_1yr_Celebrations_Rakhi',	'txn_1yr_Chains_nd_Hair_Accessories',	'txn_1yr_Charity',	'txn_1yr_Children',	'txn_1yr_Chocolates',	'txn_1yr_Christmas',	'txn_1yr_Clocks',	'txn_1yr_Clothing_nd_Shoes',	'txn_1yr_Clutches_nd_Wallets',	'txn_1yr_Coins',	'txn_1yr_Computers_nd_Peripherals',	'txn_1yr_Cooking_Essentials',	'txn_1yr_Crystals',	'txn_1yr_Cushion_Covers_nd_Pillow_Covers',	'txn_1yr_Decor',	'txn_1yr_Devotional',	'txn_1yr_Diamond_Gold',	'txn_1yr_Digital_Cameras',	'txn_1yr_Dining',	'txn_1yr_Disability',	'txn_1yr_Diwan_Sets_nd_Curtains',	'txn_1yr_Dolls',	'txn_1yr_Dryfruits',	'txn_1yr_Earrings',	'txn_1yr_Ebooks',	'txn_1yr_Education',	'txn_1yr_Elderly',	'txn_1yr_Electronics',	'txn_1yr_Employment',	'txn_1yr_Environment',	'txn_1yr_Essentials',	'txn_1yr_Fashion',	'txn_1yr_Fashion_Accessories',	'txn_1yr_Flower_Arrangements',	'txn_1yr_Flower_Arrangements_nd_Bouquets',	'txn_1yr_Flower_Bouquet',	'txn_1yr_Fragrances',	'txn_1yr_Fun_nd_Engagement',	'txn_1yr_Furnishings',	'txn_1yr_Furniture',	'txn_1yr_Garden_nd_Outdoors',	'txn_1yr_General_Merchandise',	'txn_1yr_Genesis_Vouchers',	'txn_1yr_Gift_Cards_nd_Vouchers',	'txn_1yr_Gifts',	'txn_1yr_Gold_Coin',	'txn_1yr_Gold_nd_Diamond',	'txn_1yr_Gold_Plated',	'txn_1yr_Good_Luck_Plants',	'txn_1yr_Gym_Sets',	'txn_1yr_Hair_Care',	'txn_1yr_Hampers',	'txn_1yr_Handbags',	'txn_1yr_Handbags_nd_Accessories',	'txn_1yr_Handicrafts',	'txn_1yr_Health',	'txn_1yr_Health_nd_Fitness',	'txn_1yr_Home',	'txn_1yr_Home_Entertainment',	'txn_1yr_Home_nd_Kitchen',	'txn_1yr_Home_Theaters_nd_Speakers',	'txn_1yr_Idols',	'txn_1yr_Jewellery',	'txn_1yr_Jewellery_Boxes',	'txn_1yr_Kindle',	'txn_1yr_Kitchen',	'txn_1yr_Kitchen_Tools',	'txn_1yr_Kits_nd_Hampers',	'txn_1yr_Kurtas',	'txn_1yr_Kurtis',	'txn_1yr_Lamps',	'txn_1yr_Laptops_Desktops_nd_Others',	'txn_1yr_Learning_nd_Education',	'txn_1yr_Lifestyle',	'txn_1yr_Luxury_Gift_Cards',	'txn_1yr_Men',	'txn_1yr_MenGrooming',	'txn_1yr_Metal_Alloy',	'txn_1yr_Mithai',	'txn_1yr_Mobiles',	'txn_1yr_Mothers_Day__Celebration',	'txn_1yr_Mugs',	'txn_1yr_Mugs_nd_Bottles',	'txn_1yr_Necklaces',	'txn_1yr_Non_Fiction',	'txn_1yr_Oberoi_Vouchers',	'txn_1yr_Office',	'txn_1yr_Organic_Food_nd_Groceries',	'txn_1yr_Pearls',	'txn_1yr_Pendants',	'txn_1yr_Pens',	'txn_1yr_Perfumes_nd_Deos',	'txn_1yr_Personal_Care',	'txn_1yr_Personalized_Cushion',	'txn_1yr_Photo_Frames',	'txn_1yr_Pillows_nd_Cushion_Covers',	'txn_1yr_Points_Donation',	'txn_1yr_Print',	'txn_1yr_Printers',	'txn_1yr_Puja',	'txn_1yr_Pu_Leather',	'txn_1yr_Restaurants',	'txn_1yr_Rings',	'txn_1yr_School_Bags',	'txn_1yr_Senior_Citizen_Phones',	'txn_1yr_Sets',	'txn_1yr_Shawls',	'txn_1yr_Shoes',	'txn_1yr_Silver_Coin',	'txn_1yr_Silver_nd_Gold_Plated',	'txn_1yr_Silver_Plated',	'txn_1yr_Sling_Bags',	'txn_1yr_Soft_Toys',	'txn_1yr_Sports_Games_nd_Toys',	'txn_1yr_Sports_nd_Outdoors',	'txn_1yr_State_Bank_Gift_Card',	'txn_1yr_Stationery',	'txn_1yr_Stoles',	'txn_1yr_Storage_nd_Containers',	'txn_1yr_Storage_Devices',	'txn_1yr_Strolleys',	'txn_1yr_Sunglasses',	'txn_1yr_Tableware',	'txn_1yr_Thalis_nd_Diyas',	'txn_1yr_Thread',	'txn_1yr_Toys',	'txn_1yr_Travel',	'txn_1yr_Travel_nd_Hotels',	'txn_1yr_Trouser',	'txn_1yr_T_Shirts',	'txn_1yr_Tvs',	'txn_1yr_Utilities',	'txn_1yr_Value_Phones',	'txn_1yr_Wallets',	'txn_1yr_Watches',	'txn_1yr_Western_Wear',	'txn_1yr_Women',	'txn_1yr_Youth',
'monetory','recency','frequency']].astype('uint8')
#df.dtypes
print(df.memory_usage())


# In[182]:


df_variables = df.copy()
df_variables = df_variables.drop(['bank','membershipno'], axis = 1)
#print(df.head())
#print(df_variables.head())


# In[183]:


print(sys.getsizeof(df)/1e+9)
print(df_variables.memory_usage())


# In[184]:


columns_to_be_dropped = []
columns_not_to_be_dropped = []
for column in df_variables.columns:
    if df_variables[column].sum() == 0:
        columns_to_be_dropped.append(column)
    else:
        columns_not_to_be_dropped.append(column)
        pass
        
print(len(columns_to_be_dropped))

df_variables.drop(columns_to_be_dropped, axis = 1, inplace=True)
print(len(df_variables.columns))
print(len(columns_not_to_be_dropped))


# In[186]:


qt = QuantileTransformer(n_quantiles=2000, random_state=0)
df_variables_final = pd.DataFrame(qt.fit_transform(df_variables),columns = columns_not_to_be_dropped)


# In[39]:


#df['bank_membershipno'] = df['bank'].map(str) + '_' + df['membershipno'].map(str)
#scaled_df_bk_mem = pd.concat([df[['bank','membershipno']],scaled_df],axis = 1)



# In[188]:


inertia = []

for cluster in range(1,6):
    kmean = KMeans(n_clusters = cluster, init = 'k-means++', n_init = 10, max_iter= 250, random_state = 0)
    kmean.fit(df_variables_final)
    kmean.inertia_
    inertia.append(kmean.inertia_)
    


# In[189]:


plt.figure(figsize = (12,6))
plt.scatter(range(1,6), inertia, color = 'g')
plt.xlabel('cluster_num')
plt.ylabel('inertia')


# In[190]:


clusters_num = inertia.index(min(inertia)) + 1
clusters_num


# In[192]:


kmean = KMeans(n_clusters = clusters_num, init = 'k-means++', n_init = 10, max_iter= 250, random_state = 0)
#‘k-means++’ : selects initial cluster centers for k-mean clustering in a smart way to speed up convergence.
#n_init = runs of n_init number of different intital cetroids
# max_iter no of runs it take for each cluster, n_init till convergence

kmean.fit(df_variables_final)
pred = kmean.predict(df_variables_final)
pred_frame = pd.DataFrame(pred, columns = ['clusternumber'])
#pred_frame['clusternumber'].value_counts()


# In[195]:


print(len(df)*0.35)
print(len(df)*0.05)


# In[193]:


pred_frame['clusternumber'].value_counts()


# In[194]:


clustered_variables = pd.concat([df_variables_final,pred_frame],axis = 1)
clustered_customers = pd.concat([df[['bank','membershipno']],pred_frame],axis = 1)
clustered_customers['bank_membershipno'] = clustered_customers['bank'].map(str) + '_' + clustered_customers['membershipno'].map(str)
clustered_customers.head()


# In[82]:


#cluster wise members characteristics
'''
for i in range(0,5):
    random_cluster_1 = pd.DataFrame(clustered_variables[clustered_variables['clusternumber']==i].mean())
    cluster_wise_mean = random_cluster_1.transpose() 
    cluster_wise_mean.to_csv("S\\Tanvi_Patil\\Recommendation system\\cluster_wise_mean.csv",mode = 'a')    ''' 


# In[196]:


#get the data for Apriori Algo
#gpdbconn=pyodbc.connect('Driver=SQL Server; Server='+SQLserver+'; DATABASE='+SQLdb+'; UID='+SQLuser+'; PWD='+SQLpwd+'')
gpdbconn=psycopg2.connect(host=Postgreserver, dbname=Postgredbname, user=Postgreuser, password=Postgrepassword)
gpdbcursor = gpdbconn.cursor()
gpdbcursor.execute("""select bank, membershipno, qty, category,request_dt from db.merchandise_sbi_redm
where request_dt > current_date - interval '3 yrs' """)
    
redeemers=pd.DataFrame(gpdbcursor.fetchall(),columns = ['bank','membershipno','qty','category','request_dt'])
redeemers['bank_membershipno'] = redeemers['bank'].map(str)+str('_')+redeemers['membershipno'].map(str)
redeemers = redeemers.sort_values(by=['bank_membershipno','request_dt'],ascending = [1,1])
redeemers = redeemers.set_index('bank_membershipno')
                   
gpdbconn.close()


# In[202]:


def get_cluster_txns_unique_prod(clusterno, clustered_customers, redeemers):
    df_clusterno_members = clustered_customers[clustered_customers['clusternumber']==clusterno][['bank','membershipno','bank_membershipno','clusternumber']]
    df_clusterno_txn = redeemers[redeemers.index.isin(df_clusterno_members['bank_membershipno'])]
    unique_products = list(df_clusterno_txn.category.unique())
    if None in unique_products:
        unique_products.remove(None)
    df_clusterno_txn['list_1']=df_clusterno_txn.category.apply(lambda x: pd.Series(x).to_list())*df_clusterno_txn['qty']
    df_clusterno_txn['list_2']=df_clusterno_txn['list_1']*df_clusterno_txn['qty']
    txn_cluster_agg_cat=pd.DataFrame(df_clusterno_txn.groupby(['bank','membershipno','request_dt'])['list_2'].sum())
    return unique_products, txn_cluster_agg_cat

#members_subcat_accuracy_test=members_subcat_accuracy[['bank','membershipno','subcategory_2018']].groupby(['bank','membershipno']).aggregate(lambda x: set(x.unique())).reset_index()


# In[281]:


#txn_cluster_agg_cat[txn_cluster_agg_cat['list_2']==[kitchen]]
#txn_cluster_agg_cat['list_2'].value_counts()


# In[198]:


#get support in percentage
def get_support(txn_cluster_agg_cat, unique_products):
    support = {}
    for i in range(0,len(unique_products)):
            count = sum(txn_cluster_agg_cat['list_2'].apply(lambda x : 1 if unique_products[i] in x else 0))
            support[unique_products[i]] = count*100.00/len(txn_cluster_agg_cat)
    return support

#txn_cluster_i['support'] = txn_cluster_i.groupname.value_counts()/len(txn_cluster_i)


# In[205]:


#get confidence in percentage
def get_test_confidence(cross_product,txn_cluster_agg_cat,support_tan):
    confidence = pd.DataFrame(data = cross_product, columns = ['X','Y'])
    confidence['confidence']=None
    iter = 0
    for X,Y in cross_product:
        #print(X,Y)
        count = sum(txn_cluster_agg_cat['list_2'].apply(lambda x : 1 if (X in x) & (Y in x) else 0))
        confidence['confidence'][iter] = count*10000.00/len(txn_cluster_agg_cat)/support_tan[X]
        iter+=1
    return confidence


# In[221]:


def get_confidence(clusterno, clustered_customers, redeemers):
    unique_products, txn_cluster_agg_cat = get_cluster_txns_unique_prod(clusterno, clustered_customers, redeemers)
    print(clusterno, 'Done with getting cluster txns')
    support_tan = get_support(txn_cluster_agg_cat, unique_products)
    print(clusterno, 'Done with getting support for categories')
    cross_product = list(product(unique_products,unique_products))
    confidence = get_test_confidence(cross_product,txn_cluster_agg_cat,support_tan)
    print(clusterno, 'Done with getting confidence for categories')
    confidence['support_y'] = confidence['Y'].apply(lambda x: support_tan[x]) 
    confidence['support_x'] = confidence['X'].apply(lambda x: support_tan[x]) 
    confidence['lift'] = confidence['confidence']/confidence['support_y']
    confidence['clusternumber']= clusterno
    print(clusterno, 'Done')
    return confidence


# In[246]:


#df_lift[(df_lift['X'] == 'mugs & bottles')&(df_lift['clusternumber'] == 0)&(df_lift['Y'] == 'men\'s grooming')]
#clustered_customers


# In[226]:


get_ipython().run_cell_magic('time', '', "#Find the lift of subcategories in the csv below and recommend them accordingly\ndf_lift = pd.DataFrame(columns = ['X','Y','confidence','support_y','support_x', 'lift','clusternumber'])\n\nfor i in range(4,clusters_num):\n    print(i)\n    confidence = get_confidence(i, clustered_customers, redeemers)\n    df_lift = df_lift.append(confidence)\n    print(len(df_lift))")


# In[229]:


df_lift.to_csv("S\\Tanvi_Patil\\Recommendation system\\Results\\lift_of_clusters.csv", sep='\t', header=True, index=False)


# In[247]:


clustered_customers[['bank','membershipno','clusternumber']].to_csv("S\\Tanvi_Patil\\Recommendation system\\Results\\clustered_customers.csv", sep='\t', header=True, index=False)


# In[252]:


conn=psycopg2.connect(host=Postgreserver, dbname=Postgredbname, user=Postgreuser, password=Postgrepassword)
cur = conn.cursor()
file1 = open("S\\Tanvi_Patil\\Recommendation system\\Results\\lift_of_clusters.csv")
copy_query = "COPY db.merchandise_cluster_rules FROM STDOUT csv DELIMITER '\t'  ESCAPE '\\' HEADER "  # Replace your table name in place of mem_info
cur.copy_expert(copy_query, file1)
file2 = open("S\\Tanvi_Patil\\Recommendation system\\Results\\clustered_customers.csv")
copy_query = "COPY db.merchandise_clustered_customers FROM STDOUT csv DELIMITER '\t'  ESCAPE '\\' HEADER "  # Replace your table name in place of mem_info
cur.copy_expert(copy_query, file2)
conn.commit()
cur.close()
conn.close()


# In[ ]:




