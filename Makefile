# make command will take additional argument string as MKARGS
# e.g., make test-race MKARGS="-timeout 180s"

# folder name of the package of interest
PKGNAME = raft
MKARGS = -timeout 3600s

.PHONY: build final checkpoint all final-race checkpoint-race all-race clean docs
.SILENT: build final checkpoint all final-race checkpoint-race all-race clean docs

# compile the package
build:
	cd src/$(PKGNAME); go build $(PKGNAME).go

# run tests on the package
final: build
	cd src/$(PKGNAME); go test -v $(MKARGS) -run Final

checkpoint: build
	cd src/$(PKGNAME); go test -v $(MKARGS) -run Checkpoint

all: build
	cd src/$(PKGNAME); go test -v $(MKARGS)

final-race: build
	cd src/$(PKGNAME); go test -v $(MKARGS) -race -run Final

checkpoint-race: build
	cd src/$(PKGNAME); go test -v $(MKARGS) -race -run Checkpoint

all-race: build
	cd src/$(PKGNAME); go test -v $(MKARGS) -race 

# delete executable and docs, leaving only source
clean:
	rm -rf src/$(PKGNAME)/$(PKGNAME) src/$(PKGNAME)/$(PKGNAME)-doc.txt

# generate documentation for the package
docs:
	cd src/$(PKGNAME); go doc -u -all > $(PKGNAME)-doc.txt
