from flask import Flask, request, render_template, send_file
import psycopg2
import pandas as pd
import folium
import branca.colormap as cm


import psycopg2
import geoplotlib
from geoplotlib.utils import BoundingBox, DataAccessObject
from geoplotlib.colors import ColorMap
from pandas import DataFrame
import json

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

app = Flask(__name__, static_url_path='/static')
#app.run(debug=True)

# Start of Solar plant part. For displaying Data for Solar plant capacity in Germany on the website
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/Solarquery', methods=['POST'])
def Solarquery():
   # user_query = request.form['user_query']

    # Query to get the total capacity from Solar_extended table
    query = f"""
    SELECT we."Landkreis", SUM(eeg."InstallierteLeistung") AS total_installierte_leistung
    FROM solar_extended AS we
    JOIN solar_eeg AS eeg ON we."EegMastrNummer" = eeg."EegMastrNummer"
    GROUP BY we."Landkreis";
    """

# Connecting the database: We have to change the credentials everytime we do a bulk download and create a new PG database
    connection = psycopg2.connect(
    user="tester",
    password="1234",
    host="localhost",
    port="5432",
    database="ThesisTry1"
)

    # Create a cursor and execute the query
    cursor = connection.cursor()
    cursor.execute(query)

    # Fetch all the rows from the result
    results = cursor.fetchall()

    kreisliste = DataFrame(results)
    kreisliste.columns = ['landkreis','capacity']

    # Drop rows with missing longitude or latitude values
    kreisliste = kreisliste.dropna()

    # Remove the districts where we cannot find the names
    kreisliste = kreisliste[kreisliste['landkreis'] != 'Neustadt an der Weinstraße']
    kreisliste = kreisliste[kreisliste['landkreis'] != 'Rhein-Pfalz-Kreis']
    #kreisliste.to_csv('kreiseInDatabaseneu.csv', index=False)


    #display(kreisliste)

    #öffnen der zuvor erstellten geojson datei - with dient dabei zum direkten wieder schließen
    #f=codecs.open(filename, 'r', 'utf-8')
    with open('Landkreis_Solar.json',encoding='utf8') as handle:
        country_geo = json.loads(handle.read())

    #erstellen eines neuen DF für die Landkreisliste 
    struktur = {'kreisnummer' : [],'landkreis' : []}
    kreislist = DataFrame(struktur)

    #beschreiben der kreisliste mit entsprechenden Namen aus der JSON Datei
    j = 0
    for i in country_geo['features']:
        kreislist.at[j,'landkreis']   =      i['properties']['KRG']                 #zuweisung vom Kreisnamen
        #print(i['properties']['KRG'])
        kreislist.at[j,'kreisnummer'] = float( i['properties']['SN_KRG'] ) / 1000     #zuweisung vom Kenncode -- dividieren durch 1000 weil Stammdaten aus JSON aus irgendeinem Grund drei Nullen angehängt haben
        j = j+1
        
    #kreislist['landkreis'].to_csv('kreise.csv', index=False)

    # Merge based on the "landkreis" column and select the desired columns
    merged_df = kreislist.merge(kreisliste, on='landkreis', how='inner')

    #display(merged_df)
    #merged_df['landkreis'].to_csv('dfkreise.csv', index=False)

    #print(DataFrame.merge([kreisliste,merged_df]).drop_duplicates(keep=False))
    colorStep =  [ 250, 500, 1000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,100000, 200000, 300000, 400000, 500000, 1000000, 1500000, 2000000, 2500000, 3000000]


    #map = getKarte(geoDaten = country_geo ,kreisliste = merged_df, colorstep= colorStep, key_on = 'capacity')

    #map
    geoDaten = country_geo
    key_on = 'capacity'
    colorstep = colorStep
    #Kreisnummern so verändern, dass sie mit den GoJson Nummern entsprechen
    merged_df = merged_df.astype({'kreisnummer' : 'float'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'int'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'str'})       #only strings can be verglichen werden to show results
    merged_df['kreisnummer'] = merged_df['kreisnummer'] + '000'         #add 000

    # First we determine the maximum and minimum values - this becomes the basis for the steps in the map
    vmax = merged_df[key_on].max()  
    vmin = merged_df[key_on].min() 

    #Farbverlauf einstellen -> von weiß nach blau zu Lila
    colormap = cm.LinearColormap(colors=['#ffffff', '#61abff', '#b894ff', '#a200ff', '#d000ff'], 
                             vmin=vmin,
                             vmax=vmax)

    #rückgabe des höchsten und mittelwerts in der Tabelle 
    print("max : " +str(vmax))
    print("mean: " +str(merged_df[key_on].mean()))

    #colormap so einstellen, dass überhang besteht. so werden die Daten von kleineren Landkreisen sichtbar
    colormap = colormap.to_step(len(colorstep),
                data= colorstep,
                method='quantiles',
                round_method='float')

    #karte erstellen - mittelpunkt ist mittelpunkt von deutschland
    map = folium.Map(location = [51.1657,10.4515], zoom_start=6.3)

    #Choropleth karte auf Karte auflegen
    cp = folium.Choropleth(
        geo_data=geoDaten,  #Daten für Landkreisgrenzen
        name="choropleth",  
        data= merged_df,   #kreisliste als Datensatz pbergeben
        columns=["kreisnummer", key_on],    #zeile kreisnummern zum vergleich mit GeoJson und key_on anzuzeigende Werte
        key_on = 'properties.SN_KRG',       #Ort der Kreisnummern in GeoJson
        line_weight = 0.1,
        fill_opacity= 0.1,
    ).add_to(map)

    #remove one of the two legends
    cp._children
    for key in cp._children:
        if key.startswith('color_map'):
            del(cp._children[key])
        
    #add new legend
    colormap.add_to(map)
    #schreiben der daten in die GEOJSON Datei für tooltips und zum abrufen der Werte für die Colormap
    for s in cp.geojson.data['features']:   #iterieren durch alle json features
        try: 
            row = merged_df.loc[s['properties']['SN_KRG'] == merged_df['kreisnummer']] #row index finden wo kreisnummer und SNKRG matcht
            s['properties'][key_on] = str(merged_df.at[row.index[0], key_on]) #schreiben in die JSON
        except: pass


    #set style 
    highlight_function = lambda x: {'fillColor': '#000000', 
                                'color':'#000000', 
                                'fillOpacity': 0.50, 
                                'weight': 0.1}

    style_function = lambda x: {'fillColor': '#ffffff', 
                            'color':'#000000', 
                            'fillOpacity': 0.7, 
                            'weight': 0.2,
                            'fillColor': colormap(float(x['properties'][key_on]))       #essentieller part -  ruft colormap mit wert auf
                            }

    #Zusammenfügen aller zuvor festegelgten Personalisierungen 
    NIL = folium.features.GeoJson(
        geoDaten,
        style_function=style_function, 
        control=False,
        highlight_function=highlight_function, 
        tooltip=folium.features.GeoJsonTooltip(         #tooltops sind zuvor in GeoJson List geschrieben worden
            fields=['KRG', 'SN_KRG',key_on],            #angezeigt werden kreisnamen, Kreisnummern und der Wert von key_on
            aliases=['Kreisname: ', 'Kreisnummer: ', key_on],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;") 
        )
    )
           

    map.add_child(NIL)
    map
    # Process the data and create a DataFrame (similar to the previous code)

    # Create the map as before

    # Save the map to an HTML file
    map.save("templates/district2.html") #End of Python

    # Serve the district2.html file as a response
    return send_file("templates/district2.html")

# Start of windmills part. For displaying Data for Windmills 

@app.route('/Windquery', methods=['POST'])
def Windquery():
   # user_query = request.form['user_query']

    # Query to get the total capacity from Wind_extended table
    query = f"""
    SELECT we."Landkreis", SUM(eeg."InstallierteLeistung") AS total_installierte_leistung
    FROM wind_extended AS we
    JOIN wind_eeg AS eeg ON we."EegMastrNummer" = eeg."EegMastrNummer"
    GROUP BY we."Landkreis";
    """

# Connecting the database with credentials set in PG
    connection = psycopg2.connect(
    user="tester",
    password="1234",
    host="localhost",
    port="5432",
    database="ThesisTry1"
)

    # Create a cursor and execute the query
    cursor = connection.cursor()
    cursor.execute(query)

    # Fetch all the rows from the result
    results = cursor.fetchall()

    kreisliste = DataFrame(results)
    kreisliste.columns = ['landkreis','capacity']

    # Drop rows with missing longitude or latitude values
    kreisliste = kreisliste.dropna()
    # Remove the row where the 'Name' doesn't match
    kreisliste = kreisliste[kreisliste['landkreis'] != 'Neustadt an der Weinstraße']
    kreisliste = kreisliste[kreisliste['landkreis'] != 'Rhein-Pfalz-Kreis']
    #kreisliste.to_csv('kreiseInDatabaseneu.csv', index=False)


    #display(kreisliste)

    #öffnen der zuvor erstellten geojson datei - with dient dabei zum direkten wieder schließen
    #f=codecs.open(filename, 'r', 'utf-8')
    with open('Landkreis_Windmills.json',encoding='utf8') as handle:
        country_geo = json.loads(handle.read())

    #erstellen eines neuen DF für die Landkreisliste 
    struktur = {'kreisnummer' : [],'landkreis' : []}
    kreislist = DataFrame(struktur)

    #beschreiben der kreisliste mit entsprechenden Namen aus der JSON Datei
    j = 0
    for i in country_geo['features']:
        kreislist.at[j,'landkreis']   =      i['properties']['KRG']                 #zuweisung vom Kreisnamen
        #print(i['properties']['KRG'])
        kreislist.at[j,'kreisnummer'] = float( i['properties']['SN_KRG'] ) / 1000     #zuweisung vom Kenncode -- dividieren durch 1000 weil Stammdaten aus JSON aus irgendeinem Grund drei Nullen angehängt haben
        j = j+1
        
    #kreislist['landkreis'].to_csv('kreise.csv', index=False)

    # Merge based on the "landkreis" column and select the desired columns
    merged_df = kreislist.merge(kreisliste, on='landkreis', how='inner')

    #display(merged_df)
    #merged_df['landkreis'].to_csv('dfkreise.csv', index=False)

    #print(DataFrame.merge([kreisliste,merged_df]).drop_duplicates(keep=False))
    colorStep =  [ 250, 500, 1000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,100000, 200000, 300000, 400000, 500000, 1000000, 1500000, 2000000, 2500000, 3000000]


    #map = getKarte(geoDaten = country_geo ,kreisliste = merged_df, colorstep= colorStep, key_on = 'capacity')

    #map
    geoDaten = country_geo
    key_on = 'capacity'
    colorstep = colorStep
    #Kreisnummern so verändern, dass sie mit den GoJson Nummern entsprechen
    merged_df = merged_df.astype({'kreisnummer' : 'float'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'int'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'str'})       #only strings can be verglichen werden to show results
    merged_df['kreisnummer'] = merged_df['kreisnummer'] + '000'         #add 000

    #First determining the maximum and minimum values - this becomes the basis for the steps in the map
    vmax = merged_df[key_on].max()  
    vmin = merged_df[key_on].min() 
    #Farbverlauf einstellen -> von weiß nach blau zu Lila
    colormap = cm.LinearColormap(colors=['#ffffff', '#61abff', '#b894ff', '#a200ff', '#d000ff'], 
                             vmin=vmin,
                             vmax=vmax)

    #rückgabe des höchsten und mittelwerts in der Tabelle 
    print("max : " +str(vmax))
    print("mean: " +str(merged_df[key_on].mean()))

    #colormap so einstellen, dass überhang besteht. so werden die Daten von kleineren Landkreisen sichtbar
    colormap = colormap.to_step(len(colorstep),
                data= colorstep,
                method='quantiles',
                round_method='float')

    #karte erstellen - mittelpunkt ist mittelpunkt von deutschland
    map = folium.Map(location = [51.1657,10.4515], zoom_start=6.3)

    #Choropleth karte auf Karte auflegen
    cp = folium.Choropleth(
        geo_data=geoDaten,  #Daten für Landkreisgrenzen
        name="choropleth",  
        data= merged_df,   #kreisliste als Datensatz pbergeben
        columns=["kreisnummer", key_on],    #zeile kreisnummern zum vergleich mit GeoJson und key_on anzuzeigende Werte
        key_on = 'properties.SN_KRG',       #Ort der Kreisnummern in GeoJson
        line_weight = 0.1,
        fill_opacity= 0.1,
    ).add_to(map)

    #remove one of the two legends
    cp._children
    for key in cp._children:
        if key.startswith('color_map'):
            del(cp._children[key])
        
    #add new legend
    colormap.add_to(map)
    #schreiben der daten in die GEOJSON Datei für tooltips und zum abrufen der Werte für die Colormap
    for s in cp.geojson.data['features']:   #iterieren durch alle json features
        try: 
            row = merged_df.loc[s['properties']['SN_KRG'] == merged_df['kreisnummer']] #row index finden wo kreisnummer und SNKRG matcht
            s['properties'][key_on] = str(merged_df.at[row.index[0], key_on]) #schreiben in die JSON
        except: pass


    #set style 
    highlight_function = lambda x: {'fillColor': '#000000', 
                                'color':'#000000', 
                                'fillOpacity': 0.50, 
                                'weight': 0.1}

    style_function = lambda x: {'fillColor': '#ffffff', 
                            'color':'#000000', 
                            'fillOpacity': 0.7, 
                            'weight': 0.2,
                            'fillColor': colormap(float(x['properties'][key_on]))       #essentieller part -  ruft colormap mit wert auf
                            }

    #Zusammenfügen aller zuvor festegelgten Personalisierungen 
    NIL = folium.features.GeoJson(
        geoDaten,
        style_function=style_function, 
        control=False,
        highlight_function=highlight_function, 
        tooltip=folium.features.GeoJsonTooltip(         #tooltops sind zuvor in GeoJson List geschrieben worden
            fields=['KRG', 'SN_KRG',key_on],            #angezeigt werden kreisnamen, Kreisnummern und der Wert von key_on
            aliases=['Kreisname: ', 'Kreisnummer: ', key_on],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;") 
        )
    )
           

    map.add_child(NIL)
    map
    # Process the data and create a DataFrame (similar to the previous code)

    # Create the map as before

    # Save the map to an HTML file
    map.save("templates/windmills.html")

    # Serve the district2.html file as a response
    return send_file("templates/windmills.html")

## FOr Biomass
@app.route('/biomass', methods=['POST'])
def biomass():

    # Replace with your database credentials
    db_connection = psycopg2.connect(
    user="tester",
    password="1234",
    host="localhost",
    port="5432",
    database="ThesisTry1"
    )

    #updated query
    query = """
    SELECT we."Landkreis", SUM(eeg."InstallierteLeistung") AS total_installierte_leistung
    FROM biomass_extended AS we
    JOIN biomass_eeg AS eeg ON we."EegMastrNummer" = eeg."EegMastrNummer"
    GROUP BY we."Landkreis";
    """
    # Create a cursor and execute the query
    cursor = db_connection.cursor()
    cursor.execute(query)

    # Fetch all the rows from the result
    results = cursor.fetchall()

    kreisliste = DataFrame(results)
    kreisliste.columns = ['landkreis','capacity']

    # Drop rows with missing longitude or latitude values
    kreisliste = kreisliste.dropna()
 
    #kreisliste = kreisliste[kreisliste['landkreis'] != 'Mönchengladbach']
    #kreisliste = kreisliste[kreisliste['landkreis'] != 'Südliche Weinstraße']


    #kreisliste.to_csv('kreiseBiomass.csv', index=False)


    #display(kreisliste)

    #öffnen der zuvor erstellten geojson datei - with dient dabei zum direkten wieder schließen
    #f=codecs.open(filename, 'r', 'utf-8')
    with open('Landkreise_biomass.json',encoding='utf8') as handle:
        country_geo = json.loads(handle.read())

    #erstellen eines neuen DF für die Landkreisliste 
    struktur = {'kreisnummer' : [],'landkreis' : []}
    #struktur = {'landkreis' : []}
    kreislist = DataFrame(struktur)

    #beschreiben der kreisliste mit entsprechenden Namen aus der JSON Datei
    j = 0
    for i in country_geo['features']:
        kreislist.at[j,'landkreis']   =      i['properties']['KRG']                 #zuweisung vom Kreisnamen
        #print(i['properties']['KRG'])
        kreislist.at[j,'kreisnummer'] = float( i['properties']['SN_KRG'] ) / 1000     #zuweisung vom Kenncode -- dividieren durch 1000 weil Stammdaten aus JSON aus irgendeinem Grund drei Nullen angehängt haben
        j = j+1

    #kreislist.to_csv('kreiseBiomassGeoJson.csv', index=False)

    # Merge based on the "landkreis" column and select the desired columns
    merged_df = kreislist.merge(kreisliste, on='landkreis', how='inner')
    merged_df = merged_df.dropna()
    #display(merged_df)
    #merged_df.to_csv('dfkreise.csv', index=False)
    #merged_df.to_csv('mergedkreiseBiomassGeoJson.csv', index=False)

    #print(DataFrame.merge([kreisliste,merged_df]).drop_duplicates(keep=False))
    colorStep =  [1000, 10000, 20000, 25000, 27000, 30000, 35000, 40000, 45000, 48000, 50000, 55000, 60000, 65000, 70000, 80000, 90000,100000, 150000,180000, 200000, 220000, 230000, 250000, 270000, 290000, 300000, 320000, 350000, 365000, 380000, 390000, 401000]


    geoDaten = country_geo
    key_on = 'capacity'
    colorstep = colorStep
    #Kreisnummern so verändern, dass sie mit den GoJson Nummern entsprechen
    merged_df = merged_df.astype({'kreisnummer' : 'float'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'int'})       #get rid of 0 in front
    merged_df = merged_df.astype({'kreisnummer' : 'str'})       #only strings can be verglichen werden to show results
    merged_df['kreisnummer'] = merged_df['kreisnummer'] + '000'         #add 000

    #First determining the maximum and minimum values - this becomes the basis for the steps in the map
    vmax = merged_df[key_on].max()  
    vmin = merged_df[key_on].min() 
    #Farbverlauf einstellen -> von weiß nach blau zu Lila
    colormap = cm.LinearColormap(colors=['#ffffff', '#61abff', '#b894ff', '#a200ff', '#d000ff'], 
                                vmin=vmin,
                                vmax=vmax)

    #rückgabe des höchsten und mittelwerts in der Tabelle 
    print("max : " +str(vmax))
    print("mean: " +str(merged_df[key_on].mean()))

    #colormap so einstellen, dass überhang besteht. so werden die Daten von kleineren Landkreisen sichtbar
    colormap = colormap.to_step(len(colorstep),
                data= colorstep,
                method='quantiles',
                round_method='float')

    #karte erstellen - mittelpunkt ist mittelpunkt von deutschland
    map = folium.Map(location = [51.1657,10.4515], zoom_start=6.3)

    #Choropleth karte auf Karte auflegen
    cp = folium.Choropleth(
        geo_data=geoDaten,  #Daten für Landkreisgrenzen
        name="choropleth",  
        data= merged_df,   #kreisliste als Datensatz pbergeben
        columns=["kreisnummer", key_on],    #zeile kreisnummern zum vergleich mit GeoJson und key_on anzuzeigende Werte
        key_on = 'properties.SN_KRG',       #Ort der Kreisnummern in GeoJson
        line_weight = 0.1,
        fill_opacity= 0.1,
    ).add_to(map)

    #remove one of the two legends
    cp._children
    for key in cp._children:
        if key.startswith('color_map'):
            del(cp._children[key])
        
    #add new legend
    colormap.add_to(map)
    #schreiben der daten in die GEOJSON Datei für tooltips und zum abrufen der Werte für die Colormap
    for s in cp.geojson.data['features']:   #iterieren durch alle json features
        try: 
            row = merged_df.loc[s['properties']['SN_KRG'] == merged_df['kreisnummer']] #row index finden wo kreisnummer und SNKRG matcht
            s['properties'][key_on] = str(merged_df.at[row.index[0], key_on]) #schreiben in die JSON
        except: pass


    #set style 
    highlight_function = lambda x: {'fillColor': '#000000', 
                                'color':'#000000', 
                                'fillOpacity': 0.50, 
                                'weight': 0.1}

    style_function = lambda x: {'fillColor': '#ffffff', 
                            'color':'#000000', 
                            'fillOpacity': 0.7, 
                            'weight': 0.2,
                            'fillColor': colormap(float(x['properties'][key_on]))       #essentieller part -  ruft colormap mit wert auf
                            }

    #Zusammenfügen aller zuvor festegelgten Personalisierungen 
    NIL = folium.features.GeoJson(
        geoDaten,
        style_function=style_function, 
        control=False,
        highlight_function=highlight_function, 
        tooltip=folium.features.GeoJsonTooltip(         #tooltops sind zuvor in GeoJson List geschrieben worden
            fields=['KRG', 'SN_KRG',key_on],            #angezeigt werden kreisnamen, Kreisnummern und der Wert von key_on
            aliases=['Kreisname: ', 'Kreisnummer: ', key_on],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;") 
        )
    )
            

    map.add_child(NIL)
    map
    # Process the data and create a DataFrame (similar to the previous code)

    # Create the map as before

    # Save the map to an HTML file
    map.save("templates/biomass.html")

    # Serve the district2.html file as a response
    return send_file("templates/biomass.html")


@app.route('/trend', methods=['POST'])
def trend():
   # user_query = request.form['user_query']

    # Connecting with the database with the credentials set in PG
    db_connection = psycopg2.connect(
    user="tester",
    password="1234",
    host="localhost",
    port="5432",
    database="ThesisTry1"
    )
    #updated query for extracting the sum of inverted power in different years
    query0 = """
    SELECT
        SUM(SE."ZugeordneteWirkleistungWechselrichter") AS Inverted_power_2019,SUM(SE."Bruttoleistung") as Total_Power_2019
    FROM
        solar_extended as SE
    WHERE
        SUBSTRING(CAST(SE."Inbetriebnahmedatum" AS TEXT) FROM 1 FOR 4) = '2019';
    """
    query1 = """
    SELECT
        SUM(SE."ZugeordneteWirkleistungWechselrichter") AS Inverted_power_2020, SUM(SE."Bruttoleistung") as Total_Power_2020
    FROM
        solar_extended as SE
    WHERE
        SUBSTRING(CAST(SE."Inbetriebnahmedatum" AS TEXT) FROM 1 FOR 4) = '2020';
    """
    query2 = """
    SELECT
        SUM(SE."ZugeordneteWirkleistungWechselrichter") AS Inverted_power_2021, SUM(SE."Bruttoleistung") as Total_Power_2021
    FROM
        solar_extended as SE
    WHERE
        SUBSTRING(CAST(SE."Inbetriebnahmedatum" AS TEXT) FROM 1 FOR 4) = '2021';
    """
    query3 = """
    SELECT
        SUM(SE."ZugeordneteWirkleistungWechselrichter") AS Inverted_power_2022, SUM(SE."Bruttoleistung") as Total_Power_2022
    FROM 
        solar_extended as SE
    WHERE
        SUBSTRING(CAST(SE."Inbetriebnahmedatum" AS TEXT) FROM 1 FOR 4) = '2022';
    """
    query4 = """
    SELECT
        SUM(SE."ZugeordneteWirkleistungWechselrichter") AS Inverted_power_2023, SUM(SE."Bruttoleistung") as Total_Power_2023
    FROM
        solar_extended as SE
    WHERE
        SUBSTRING(CAST(SE."Inbetriebnahmedatum" AS TEXT) FROM 1 FOR 4) = '2023';
    """


    # Create a cursor and execute the query
    cursor0 = db_connection.cursor()
    cursor0.execute(query0)

    cursor1 = db_connection.cursor()
    cursor1.execute(query1)

    cursor2 = db_connection.cursor()
    cursor2.execute(query2)

    cursor3 = db_connection.cursor()
    cursor3.execute(query3)

    cursor4 = db_connection.cursor()
    cursor4.execute(query4)

    capacity0 = cursor0.fetchall()
    capacity1 = cursor1.fetchall()
    capacity2 = cursor2.fetchall()
    capacity3 = cursor3.fetchall()
    capacity4 = cursor4.fetchall()
    capacity = [capacity0[0][0], capacity1[0][0], capacity2[0][0], capacity3[0][0], capacity4[0][0]]
    totalpower = [capacity0[0][1], capacity1[0][1], capacity2[0][1], capacity3[0][1], capacity4[0][1]]
    year = ["2019","2020", "2021", "2022", "2023"]

    x_pos = np.arange(len(year))


    # Width of the bars
    bar_width = 0.35

    # Create bars for Inverted Power
    bars1 = plt.bar(x_pos - bar_width/2, capacity, width=bar_width, label='Inverted Power', color=(0.5, 0.1, 0.5, 0.6))

    # Create bars for Total Power
    bars2 = plt.bar(x_pos + bar_width/2, totalpower, width=bar_width, label='Total Power', color=(0.1, 0.5, 0.5, 0.6))

    # Set the tick positions on the x-axis
    plt.xticks(x_pos, year)

    # Add values on top of the bars for Inverted Power
    for bar, value in zip(bars1, capacity):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{value:.2f}',
                ha='center', va='bottom', fontsize=8, color='black')

    # Add values on top of the bars for Total Power
    for bar, value in zip(bars2, totalpower):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{value:.2f}',
                ha='center', va='bottom', fontsize=8, color='black')

    # Add title and axis names
    plt.title('Trend analysis of Inverted Power and Total Power per year')
    plt.xlabel('Year')
    plt.ylabel('Power')
    #plt.ticklabel_format(style='plain')
    plt.savefig('templates/trend.jpg')

    return send_file("templates/trendanalysis.html")


if __name__ == '__main__':
    app.run()