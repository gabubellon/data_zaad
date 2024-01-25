start:
	composer-dev start

stop:
	composer-dev stop

recreate:
	composer-dev restart

restart:stop start

test:
	cd ./data_zaad;python -m pytest -v -p no:warnings;cd ..