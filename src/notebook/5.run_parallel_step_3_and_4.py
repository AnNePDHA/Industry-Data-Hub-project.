# Databricks notebook source
'''
Purpose: run step 3 (tss processing) and 4 (boscar processing) in parallel
'''


from multiprocessing.pool import ThreadPool
pool = ThreadPool(5)
notebooks = ['3.tss_transform', '4.process_for_chris_and_chapr']
pool.map(lambda path: dbutils.notebook.run("/src/notebook/"+path, timeout_seconds= 1000000, arguments={"input-data": path}),notebooks)