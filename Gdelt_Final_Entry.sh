#!/bin/bash
echo "<<<Gdelt_Pinal_Project Entry>>>"
read -e -p ">>> Input EventCode : " EventCode
echo " EventCode => " $EventCode
read -e -p ">>> Input SQLDATE(keyin 'ALL' with date between 20170601~20171231) : " SQLDATE
echo " SQLDATE => " $SQLDATE
echo "Processing ....."
spark-submit --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 GDELT_Final_Project.py $EventCode $SQLDATE