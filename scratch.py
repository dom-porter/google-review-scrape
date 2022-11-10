start_time = time()
all_results = []
# scrape and crawl
with ThreadPoolExecutor(max_workers=4) as executor:
    results_futures = {executor.submit(scrape_business, target): target for target in
                       all_targets}
    for future in concurrent.futures.as_completed(results_futures):
        result = results_futures[future]
        try:
            data = future.result()
            all_results.append(data)
        except Exception as exc:
            print(exc)

stores_pt = pd.concat(all_results, ignore_index=True)
stores_pt.to_csv(f"popular_times.csv", index=False)

end_time = time()
elapsed_time = end_time - start_time
print(f"Elapsed run time: {elapsed_time} seconds")



202,Zara store UK BS1 3BX
205,Zara store UK NW4 3FP
206,Zara store UK WD17 2TB
207,Zara store UK E20 1EJ
