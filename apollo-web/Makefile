APP_CSS = ./src/main/webapp/console/css/app.css
APP_LESS = ./src/main/less/app.less
DATE=$(shell date +%I:%M%p)
CHECK=\033[32m✔\033[39m
HR====================================================

#
# BUILD it.
#
build:
	@echo "\n${HR}"
	@echo "Building..."
	@echo "${HR}\n"
	@recess --compile ${APP_LESS} > ${APP_CSS}
	@echo "Compiling LESS with Recess...               ${CHECK} Done"
	@echo "\n${HR}"
	@echo "Successfully built at ${DATE}."
	@echo "${HR}\n"

#
# recess & uglifyjs are required
#

bootstrap:
	recess --compile  ${APP_LESS} > src/main/webapp/console/css/app.css
	recess --compress ${APP_LESS} > src/main/webapp/console/css/app.min.css

watch:
.PHONY: watch