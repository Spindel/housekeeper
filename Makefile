### Configuration

IMAGE_REPO = registry.gitlab.com/modioab/housekeeper
IMAGE_ARCHIVE = image.tar

IMAGE_FILES += wheel
CLEANUP_FILES += wheel

.DEFAULT_GOAL = all
include build.mk

wheel:
	pip3 wheel --wheel-dir=wheel -r requirements.txt .

### Standard targets

.PHONY: check
check:
	$(Q)flake8 housekeeper; $(RECORD_TEST_STATUS) \
	mypy .; $(RECORD_TEST_STATUS) \
	$(RETURN_TEST_STATUS)

.PHONY: test
test:
	$(Q)python3 setup.py test
