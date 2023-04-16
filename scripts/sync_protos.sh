rm protos/*.proto

wget https://raw.githubusercontent.com/kaspanet/kaspad/master/infrastructure/network/netadapter/server/grpcserver/protowire/p2p.proto -P protos/
wget https://raw.githubusercontent.com/kaspanet/kaspad/master/infrastructure/network/netadapter/server/grpcserver/protowire/rpc.proto -P protos/
wget https://raw.githubusercontent.com/kaspanet/kaspad/master/infrastructure/network/netadapter/server/grpcserver/protowire/messages.proto -P protos/

python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/*
