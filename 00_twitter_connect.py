# Databricks notebook source
consumer_key = dbutils.secrets.get("twitter_cred", "apikey")
consumer_secret = dbutils.secrets.get("twitter_cred", "apisecret")
access_token = dbutils.secrets.get("twitter_cred", "accesstoken")
access_token_secret = dbutils.secrets.get("twitter_cred", "accesstokensecret")
bearer_token = dbutils.secrets.get("twitter_cred", "bearertoken")
