#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing Dependencies 
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


# In[2]:


#Reading dataset 1 - FAO
csv_file = "FAO.csv"
fao_data_df = pd.read_csv(csv_file,encoding='latin1')


# In[3]:


#Insert an id column and rename column 'Item' to 'FAO_Category' for later merge
fao_data_df.insert(0, 'ID', range(0, 0 + len(fao_data_df)))
fao_data_df = fao_data_df.rename(columns={'Item':'FAO_Category'})
fao_data_df.head()


# In[4]:


#Creating unique list of categories to normalize
FAO_ctgry_df = pd.DataFrame({'FAO_Category': list(set(fao_data_df['FAO_Category']))})
FAO_ctgry_df.to_csv('FAO_ctgry_df.csv')
FAO_ctgry_df.head()


# In[5]:


#read category datagrame with new column set index to normalized category
csv_file = "FAO_norm_ctgry_df.csv"
FAO_nrm_ctgry_df = pd.read_csv(csv_file)
#list(FAO_nrm_ctgry_df.columns.values)
del FAO_nrm_ctgry_df['Unnamed: 0']
del FAO_nrm_ctgry_df['Unnamed: 3']
del FAO_nrm_ctgry_df['Unnamed: 4']
#FAO_nrm_ctgry_df = FAO_nrm_ctgry_df.rename(columns={'Unnamed: 2': 'normalized_category'})
#FAO_nrm_ctgry_df = FAO_nrm_ctgry_df.set_index('normalized_category')
FAO_nrm_ctgry_df.head()


# In[6]:


# merge to add normalized category to FAO
fao_norm_df = pd.merge(fao_data_df, FAO_nrm_ctgry_df, on='FAO_Category', how='left')
fao_norm_df


# In[ ]:


#Reading open foods csv
df = pd.read_csv("en.openfoodfacts.org.products.csv")


# In[ ]:


list(df.columns.values)


# In[ ]:


#Restricitng open foods dataset
newdf = df[['product_name','categories_en', 'energy_100g', 'proteins_100g', 'carbohydrates_100g', 
                'fat_100g', 'saturated-fat_100g', 'fiber_100g', 'sugars_100g','countries','countries_en','brands','brands_tags','categories','categories_tags','categories_en', 'cholesterol_100g','trans-fat_100g','main_category_en']]
newdf.head()


# In[ ]:


#Droping NA
df2=newdf.dropna()


# In[ ]:


#Deleting Repeating rows
cleaned_df = df2[df2['product_name']!='product_name'].copy().round(2)
cleaned_df.head()


# In[ ]:


#Creating list of categories to normalize
cleaned_df['main_category_en'].unique()


# In[ ]:


a = pd.Series(['Sugary snacks', 'fr:Boulange', 'Meals', 'Chips and fries',
       'Dairies', 'Plant-based foods and beverages', 'Groceries',
       'Pates-a-tartiner-aux-noisettes-et-au-cacao', 'Spreads',
       'Beverages', 'Fruit juices', 'Meats', 'Salty snacks',
       'Dietary supplements', 'Cake-mix', 'Mashed-potatoes',
       'Granola-mix', 'Mix', 'Coconut-creams', 'Coconut-waters',
       'Seafood', 'Beef-jerky', 'Banana-nut-loaf', 'Breakfasts',
       'Desserts', 'Balsamic-vinaigrette-dressing', 'Macaroni', 'Penne',
       'Peanut-butter-bliss', 'Beef-broth', 'Farming products', 'Soymilk',
       'Genoa-salami', 'Cottage-cheese', 'Popping-corn',
       'Goat-milk-cheese', 'Salsa', 'Garlic-mashed-potatoes',
       'Chocolate-with-raspberries', 'Tartar-sauce',
       'Whole-wheat-tortillas', 'Sweeteners', 'Frozen foods',
       'Canned-precooked-meat', 'Precooked-meat', 'Potato-crips',
       'Baked-beans', 'Senate-bean-soup', 'Freeze-dried foods',
       'Pierogies', 'Black-truffle-oil', 'Coconut-cream', 'Ramen',
       'Turkey-bacon', 'Stuffing', 'Snacks', 'fr:Produits-sous-licence',
       'Hot-cocoa-mix', 'Aliments-d-origine-vegetale', 'Canned foods',
       'fr:Tartinade-amandes-et-chocolat-noir', 'Cinnamon-raisin-bagels',
       'Pancakes-and-waffles', 'Hot-cocoa-powder', 'fr:Confit-de-dinde',
       'Pies', 'fr:Breuvage', 'Cheddar-baguettes', 'White-quinoa',
       'fr:Croustilles-chips', 'fr:Biscuits-aux-brisures-de-chocolat',
       'Marinara-sauce', 'Breaded products', 'fr:Kraft',
       'fr:Melange-a-soupe', 'fr:Mayonnaise-a-l-huile-d-olive',
       'fr:Miche-de-pain-fait-d-huile-d-olive', 'Feta-cheese', 'Matzos',
       'es:Sopas-instantaneas', 'Anchovy-paste', 'Cream-horseradish',
       'Horseradish-sauce', 'Hot-dog-mustard',
       'Blue-cheese-yogurt-dressing', 'Bruschetta', 'Cactus',
       'Fat-free-milk', 'Hot-fudge', 'Prosciutto',
       'fr:Tartinade-de-fromage-fondu', 'Capers', 'Pickles',
       'Hawaiian-sweet-rolls', 'fr:Pain-grains-germes-entiers',
       'Flour-tortillas', 'Mozzarella-sticks', 'Corn-tortilla-chips',
       'Cherry-pie-filling', 'Coffee-creamer', 'Salsa-chips',
       'Goat-milk-butter', 'Juices', 'Palm-sugar', 'Nonpareil-capers',
       'Cashews', 'Juice-drinks', 'Snack', 'Waffles', 'Whipping-cream',
       'fr:Quartiers-de-pamplemousse-rouge-prepare-dans-de-l-eau-legerement-sucree',
       'Diced-tomatoes', 'Fruit-and-nut-mix',
       'Milk-chocolate-filled-with-caramel', 'Green olives',
       'Chocolate-mints', 'Dry-seasoning-mix', 'Marshmallow-creme',
       'de:Blutenwasser', 'de:Rosenwasser', 'fr:Super-fruit',
       'fr:Viande-de-porc-preparee', 'fr:Burgers-de-boeuf', 'Tortillas',
       'fr:Marmelade-aux-figues', 'Pain-pita-sucres',
       'fr:Preparation-cremeuse-de-soya-pour-cuisiner',
       'fr:Pizza-toute-garnie-avec-croute-a-l-huile-d-olive',
       'fr:Le-chenevis-designe-la-graine-de-chanvre',
       'Seed-style-mustard', 'Vinegars', 'Sunflower-spread', 'Chocolat',
       'fr:Cretons', 'fr:Galette-de-boeuf', 'Tortilla-chips',
       'Indian-food', 'Curries', 'Kale-salads', 'fr:Beignes', 'Goat-milk',
       'fr:Tartinade', 'fr:Jus-de-pommes-et-fraises', 'Indian',
       'Breakfast-granola', 'Almond-milk-yogurt', 'Chips',
       'Cheese-analogues', 'fr:Sans-produit-laitier', 'fr:Simili-yogourt',
       'Chocolats', 'Hidden-valley', 'Pecans', 'fr:Oleagineux',
       'fr:Barre-nutritive', 'fr:Saucisson-aux-noix', 'Fresh foods',
       'Olives', 'fr:Beurre-de-noix-avec-noix-de-cajou', 'es:Atun',
       'es:Pan-dulce', 'es:Crema-de-cacahuate', 'es:Dulce-de-leche',
       'pt:Biscoito-salgado', 'fr:Biscoito',
       'fr:Sirop-d-erable-en-flocons', 'fr:Turrones', 'nl:Broodbeleggen',
       'fr:Pois-verts-au-wasabi', 'fr:Vermicelle', 'Spring-roll-pastry'])


a.to_csv("./foodcategories2.csv")


# In[ ]:


#read category datagrame with new column set index to normalized category
csv_file_b = "foodcategories.csv"
Openfoods_nrm_ctgry_df = pd.read_csv(csv_file_b)
#list(FAO_nrm_ctgry_df.columns.values)
#del FAO_nrm_ctgry_df['Unnamed: 0']
#del FAO_nrm_ctgry_df['Unnamed: 3']
#del FAO_nrm_ctgry_df['Unnamed: 4']
Openfoods_nrm_ctgry_df = Openfoods_nrm_ctgry_df.rename(columns={'Food_Categories': 'main_category_en'})
#FAO_nrm_ctgry_df = FAO_nrm_ctgry_df.set_index('normalized_category')
Openfoods_nrm_ctgry_df.head()


# In[ ]:


# Merging normalized categories to the cleaned open foods dataframe
ofd_clean_norm_df = pd.merge(cleaned_df, Openfoods_nrm_ctgry_df, on='main_category_en', how='left')
ofd_clean_norm_df.head()


# In[ ]:


#Merging the two normalized datasets
ofd_fao_norm_df = pd.merge(ofd_clean_norm_df, fao_norm_df, on='Normalized_Category', how='left')
ofd_fao_norm_df.head()


# In[ ]:


# sum of merged data frames grouped by normalized category
ctgry_grp_sum = ofd_fao_norm_df.groupby('Normalized_Category').sum()
del ctgry_grp_sum['ID']
del ctgry_grp_sum['Area Code']
del ctgry_grp_sum['Element Code']
del ctgry_grp_sum['latitude']
del ctgry_grp_sum['longitude']
ctgry_grp_sum.head()

