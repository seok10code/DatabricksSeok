# Databricks notebook source
import json, random

geo_data = [{"city" : "Sydney", "country" : "Australia", "countrycode3" : "AUS", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Sofia", "country" : "Bulgaria", "countrycode3" : "BGR", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Calgary", "country" : "Canada", "countrycode3" : "CAN", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Shantou", "country" : "China", "countrycode3" : "CHN", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Giza", "country" : "Egypt", "countrycode3" : "EGY", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Munich", "country" : "Germany", "countrycode3" : "DEU", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Chennai", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Jaipur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Nagpur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Tehran", "country" : "Iran", "countrycode3" : "IRN", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Hiroshima", "country" : "Japan", "countrycode3" : "JPN", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Kuala Lumpur", "country" : "Malaysia", "countrycode3" : "MYS", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Fez", "country" : "Morocco", "countrycode3" : "MAR", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Maputo", "country" : "Mozambique", "countrycode3" : "MOZ", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Mandalay", "country" : "Myanmar", "countrycode3" : "MMR", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Gujranwala", "country" : "Pakistan", "countrycode3" : "PAK", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Manila", "country" : "Philippines", "countrycode3" : "PHL", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Riyadh", "country" : "Saudi Arabia", "countrycode3" : "SAU", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Dakar", "country" : "Senegal", "countrycode3" : "SEN", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Dubai", "country" : "United Arab Emirates", "countrycode3" : "ARE", "StateProvince" : "None", "PostalCode" : "None"},
{"city" : "Fresno", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "93650"},
{"city" : "Cincinnati", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Ohio", "PostalCode" : "41073"},
{"city" : "San Diego", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "91945"},
{"city" : "Portland", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Oregon", "PostalCode" : "97035"},
{"city" : "Long Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90712"},
{"city" : "San Antonio", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "78006"},
{"city" : "Kansas City", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "64030"},
{"city" : "Los Angeles", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90001"},
{"city" : "Memphis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Tennessee", "PostalCode" : "37501"},
{"city" : "Tucson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Arizona", "PostalCode" : "85641"},
{"city" : "Rochester", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "14602"},
{"city" : "Denver", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Colorado", "PostalCode" : "80014"},
{"city" : "Virginia Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Virginia", "PostalCode" : "23450 "},
{"city" : "Montgomery", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "36043"},
{"city" : "Plano", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "75023"},
{"city" : "Huntington", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "11721"},
{"city" : "Henderson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Nevada", "PostalCode" : "89002"},
{"city" : "St. Paul", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Minnesota", "PostalCode" : "55101"},
{"city" : "Birmingham", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "35005"},
{"city" : "St. Louis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "63101"}];

def add_geo_data(event):
    event_data = json.loads(event.data)
    event_data["geolocation"] = random.choice(geo_data)
    return json.dumps(event_data)
