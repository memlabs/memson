FROM rust:1.61 as build

# create a new empty shell project
RUN USER=root cargo new --bin memson
WORKDIR /memson

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./src ./src

# build for release
RUN rm ./target/release/deps/memson*
RUN cargo build --release

# our final base
FROM rust:1.61

# copy the build artifact from the build stage
COPY --from=build /memson/target/release/memson .

# set the startup command to run your binary
CMD ["./memson"]