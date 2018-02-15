### Configuration

ARCHIVE_PREFIX = /srv/app
SOURCE_ARCHIVE = source.tar


IMAGE_REPO = registry.gitlab.com/modioab/housekeeper
IMAGE_ARCHIVE = image.tar
IMAGE_FILES += $(SOURCE_ARCHIVE)

.DEFAULT_GOAL = all
include build.mk


### Standard targets

.PHONY: check
check:
	$(Q)python3 setup.py flake8

.PHONY: test
test:
	$(Q)python3 setup.py test
