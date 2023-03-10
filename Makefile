blast-dbf: ./suspyro/dbc_converter/blast-dbf.c ./suspyro/dbc_converter/blast.c ./suspyro/dbc_converter/blast.h
	cc -o ./suspyro/dbc_converter/blast-dbf ./suspyro/dbc_converter/blast.c ./suspyro/dbc_converter/blast-dbf.c

test: ./suspyro/dbc_converter/blast-dbf
	./suspyro/dbc_converter/blast-dbf < sids.dbc | cmp - sids.dbf

clean:
	rm -f ./suspyro/dbc_converter/blast-dbf *.o
