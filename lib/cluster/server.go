package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/uol/mycenae/lib/gorilla"
	pb "github.com/uol/mycenae/lib/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

type server struct {
	storage    *gorilla.Storage
	grpcServer *grpc.Server
}

func newServer(conf Config, strg *gorilla.Storage) (*server, error) {

	s := &server{storage: strg}

	go func(s *server, conf Config) {
		for {
			grpcServer, lis, err := s.connect(conf)
			if err != nil {
				grpclog.Printf("Unable to connect: %v", err)
				time.Sleep(time.Second)
				continue
			}
			s.grpcServer = grpcServer
			err = s.grpcServer.Serve(lis)
			if err != nil {
				grpclog.Printf("grpc server problem: %v", err)
				s.grpcServer.Stop()
				time.Sleep(time.Second)
				continue
			}

		}
	}(s, conf)

	return s, nil
}

func (s *server) connect(conf Config) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		return nil, nil, err
	}

	//c, err := newServerTLSFromFile(conf.Consul.CA, conf.Consul.Cert, conf.Consul.Key)
	logger.Debugf("loading server keys: %v %v", conf.Consul.Cert, conf.Consul.Key)
	c, err := credentials.NewServerTLSFromFile(conf.Consul.Cert, conf.Consul.Key)
	if err != nil {
		return nil, nil, err
	}
	opts := []grpc.ServerOption{grpc.Creds(c)}

	gServer := grpc.NewServer(opts...)

	pb.RegisterTimeseriesServer(gServer, s)

	return gServer, lis, nil

}

func (s *server) Write(ctx context.Context, p *pb.TSPoint) (*pb.TSErr, error) {

	logger.WithFields(logrus.Fields{
		"package": "cluster",
		"func":    "server/Write",
	}).Debugf("grpc server writing ksid=%v tsid=%v", p.GetKsid(), p.GetTsid())
	err := s.storage.Write(
		p.GetKsid(),
		p.GetTsid(),
		p.GetDate(),
		p.GetValue(),
	)

	if err != nil {
		return &pb.TSErr{Err: err.Error()}, err
	}

	logger.WithFields(logrus.Fields{
		"package": "cluster",
		"func":    "server/Write",
	}).Debugf("grpc server wrote ksid=%v tsid=%v", p.GetKsid(), p.GetTsid())
	return &pb.TSErr{Err: ""}, nil

}

func (s *server) Read(ctx context.Context, q *pb.Query) (*pb.Response, error) {

	pts, err := s.storage.Read(q.GetKsid(), q.GetTsid(), q.GetStart(), q.GetEnd())

	return &pb.Response{Pts: pts}, err
}

func newServerTLSFromFile(cafile, certfile, keyfile string) (credentials.TransportCredentials, error) {

	cp := x509.NewCertPool()

	data, err := ioutil.ReadFile(cafile)

	if err != nil {
		return nil, fmt.Errorf("Failed to read CA file: %v", err)
	}

	if !cp.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("Failed to parse any CA certificates")
	}

	cert, err := tls.LoadX509KeyPair(certfile, keyfile)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(
		&tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            cp,
			InsecureSkipVerify: true,
			//ClientAuth:   tls.RequireAndVerifyClientCert,
		}), nil

}
