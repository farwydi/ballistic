FROM earthly/dind:alpine
WORKDIR /workspace

RUN apk add curl go

project-files:
    COPY go.* ./
    RUN go mod download
    COPY --dir queue sender ./
    COPY *.go ./

test:
    FROM +project-files
    COPY docker-compose.yml ./
    COPY --dir scripts ./
    WITH DOCKER --compose docker-compose.yml
        RUN sh ./scripts/wait.sh &&\
            go test -race --tags=integration ./... -covermode=atomic -coverpkg=./... -coverprofile=coverage.txt
    END
    SAVE ARTIFACT ./coverage.txt AS LOCAL ./coverage.txt
