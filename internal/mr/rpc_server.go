package mr

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
)

func StartCoordinatorRPC(c *Coordinator, addr string) (net.Listener, error) {
	if addr == "" {
		return nil, errors.New("rpc listen address cannot be empty")
	}

	server := rpc.NewServer()
	if err := server.RegisterName("Coordinator", c); err != nil {
		return nil, fmt.Errorf("register coordinator rpc server: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for coordinator rpc: %w", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}

				return
			}

			go server.ServeConn(conn)
		}
	}()

	return listener, nil
}
