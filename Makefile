all: clean prepare run_memory_fit run_no_memory_fit

prepare:
	cd source && python generate_data_.py

run_memory_fit:
	python task1_tx_memory_fit.py

run_no_memory_fit:
	python task1_tx_no_memory_fit.py
clean:
	rm -f source/*.csv
