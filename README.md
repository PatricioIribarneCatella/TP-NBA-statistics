# TP-NBA-statistics

NBA statistics (Sistemas Distribuidos I)

### Ejecutar

- Dependencias: [_virtualenv_](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/) y [_ZeroMQ_](http://zeromq.org/) (en particular el _binding_ para [_Python_](https://pyzmq.readthedocs.io/en/latest/))

#### Cliente

```bash
 $./run-client.py

	Se generará un directorio "stats"
	 con las estadísticas
```
En otra terminal se ejecuta el servidor:

#### Servidor

```bash
 $./run-server.py [--mworkers=NUM(1)     |
		   --mreducers=NUM(2)    | 
		   --topkworkers=NUM(1)  |
		   --topkreducers=NUM(2) |
		   --ltworkers=NUM(2)    |
		   --lpworkers=NUM(2)    |]

	Argumentos:
	  - mworkers: Match Summary workers
	  - mreducers: Match Summary reducers
	  - topkworkers: Top K workers
	  - topkreducers: Top K reducers
	  - ltworkers: Local Team workers
	  - lpworkers: Local Points workers
```
Para terminar los nodos, ejecutar:

```bash
 $./kill-server.py
```

