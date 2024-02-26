// support for generic Remote Object services over sockets
// including a socket wrapper that can drop and/or delay messages arbitrarily
// works with any* objects that can be gob-encoded for serialization
//
// the LeakySocket wrapper for net.Conn is provided in its entirety, and should
// not be changed, though you may extend it with additional helper functions as
// desired.  it is used directly by the test code.
//
// the RemoteObjectError type is also provided in its entirety, and should not
// be changed.
//
// suggested RequestMsg and ReplyMsg types are included to get you started,
// but they are only used internally to the remote library, so you can use
// something else if you prefer
//
// the Service type represents the callee that manages remote objects, invokes
// calls from callers, and returns suitable results and/or remote errors
//
// the StubFactory converts a struct of function declarations into a functional
// caller stub by automatically populating the function definitions.
//
// USAGE:
// the desired usage of this library is as follows (not showing all error-checking
// for clarity and brevity):
//
//  example ServiceInterface known to both client and server, defined as
//  type ServiceInterface struct {
//      ExampleMethod func(int, int) (int, remote.RemoteObjectError)
//  }
//
//  1. server-side program calls NewService with interface and connection details, e.g.,
//     obj := &ServiceObject{}
//     srvc, err := remote.NewService(&ServiceInterface{}, obj, 9999, true, true)
//
//  2. client-side program calls StubFactory, e.g.,
//     stub := &ServiceInterface{}
//     err := StubFactory(stub, 9999, true, true)
//
//  3. client makes calls, e.g.,
//     n, roe := stub.ExampleMethod(7, 14736)
//
//
//
//
//
// TODO *** here's what needs to be done for Lab 1:
//  1. create the Service type and supporting functions, including but not
//     limited to: NewService, Start, Stop, IsRunning, and GetCount (see below)
//
//  2. create the StubFactory which uses reflection to transparently define each
//     method call in the client-side stub (see below)
//

package remote

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"time"
)

const (
	RunningProtocol string = "tcp"
)

// LeakySocket
//
// LeakySocket is a wrapper for a net.Conn connection that emulates
// transmission delays and random packet loss. it has its own send
// and receive functions that together mimic an unreliable connection
// that can be customized to stress-test remote service interactions.
type LeakySocket struct {
	s         net.Conn
	isLossy   bool
	lossRate  float32
	msTimeout int
	usTimeout int
	isDelayed bool
	msDelay   int
	usDelay   int
}

// builder for a LeakySocket given a normal socket and indicators
// of whether the connection should experience loss and delay.
// uses default loss and delay values that can be changed using setters.
func NewLeakySocket(conn net.Conn, lossy bool, delayed bool) *LeakySocket {
	ls := &LeakySocket{}
	ls.s = conn
	ls.isLossy = lossy
	ls.isDelayed = delayed
	ls.msDelay = 2
	ls.usDelay = 0
	ls.msTimeout = 500
	ls.usTimeout = 0
	ls.lossRate = 0.05

	return ls
}

// send a byte-string over the socket mimicking unreliability.
// delay is emulated using time.Sleep, packet loss is emulated using RNG
// coupled with time.Sleep to emulate a timeout
func (ls *LeakySocket) SendObject(obj []byte) (bool, error) {
	if obj == nil {
		return true, nil
	}

	if ls.s != nil {
		rand.Seed(time.Now().UnixNano())
		if ls.isLossy && rand.Float32() < ls.lossRate {
			time.Sleep(time.Duration(ls.msTimeout)*time.Millisecond + time.Duration(ls.usTimeout)*time.Microsecond)
			return false, nil
		} else {
			if ls.isDelayed {
				time.Sleep(time.Duration(ls.msDelay)*time.Millisecond + time.Duration(ls.usDelay)*time.Microsecond)
			}
			_, err := ls.s.Write(obj)
			if err != nil {
				return false, errors.New("SendObject Write error: " + err.Error())
			}
			return true, nil
		}
	}
	return false, errors.New("SendObject failed, nil socket")
}

// receive a byte-string over the socket connection.
// no significant change to normal socket receive.
func (ls *LeakySocket) RecvObject() ([]byte, error) {
	if ls.s != nil {
		buf := make([]byte, 4096)
		n := 0
		var err error
		for n <= 0 {
			n, err = ls.s.Read(buf)
			if n > 0 {
				return buf[:n], nil
			}
			if err != nil {
				if err != io.EOF {
					return nil, errors.New("RecvObject Read error: " + err.Error())
				}
			}
		}
	}
	return nil, errors.New("RecvObject failed, nil socket")
}

// enable/disable emulated transmission delay and/or change the delay parameter
func (ls *LeakySocket) SetDelay(delayed bool, ms int, us int) {
	ls.isDelayed = delayed
	ls.msDelay = ms
	ls.usDelay = us
}

// change the emulated timeout period used with packet loss
func (ls *LeakySocket) SetTimeout(ms int, us int) {
	ls.msTimeout = ms
	ls.usTimeout = us
}

// enable/disable emulated packet loss and/or change the loss rate
func (ls *LeakySocket) SetLossRate(lossy bool, rate float32) {
	ls.isLossy = lossy
	ls.lossRate = rate
}

// close the socket (can also be done on original net.Conn passed to builder)
func (ls *LeakySocket) Close() error {
	return ls.s.Close()
}

// RemoteObjectError
//
// RemoteObjectError is a custom error type used for this library to identify remote methods.
// it is used by both caller and callee endpoints.
type RemoteObjectError struct {
	Err string
}

// getter for the error message included inside the custom error type
func (e *RemoteObjectError) Error() string { return e.Err }

// RequestMsg (this is only a suggestion, can be changed)
//
// RequestMsg represents the request message sent from caller to callee.
// it is used by both endpoints, and uses the reflect package to carry
// arbitrary argument types across the network.
type RequestMsg struct {
	Method string
	Args   []interface{} // TODO: keep this as reflect.Value
}

// ReplyMsg
//
// ReplyMsg represents the reply message sent from callee back to caller
// in response to a RequestMsg. it similarly uses reflection to carry
// arbitrary return types along with a success indicator to tell the caller
// whether the call was correctly handled by the callee. also includes
// a RemoteObjectError to specify details of any encountered failure.
type ReplyMsg struct {
	Success bool
	Reply   []interface{} // TODO: keep this as reflect.Value
	Err     RemoteObjectError
}

// Service -- server side stub/skeleton
//
// A Service encapsulates a multithreaded TCP server that manages a single
// remote object on a single TCP port, which is a simplification to ease management
// of remote objects and interaction with callers.  Each Service is built
// around a single struct of function declarations. All remote calls are
// handled synchronously, meaning the lifetime of a connection is that of a
// sinngle method call.  A Service can encounter a number of different issues,
// and most of them will result in sending a failure response to the caller,
// including a RemoteObjectError with suitable details.
type Service struct {
	// TODO: populate with needed contents including, but not limited to:
	//       - reflect.Type of the Service's interface (struct of Fields)
	//       - reflect.Value of the Service's interface
	//       - reflect.Value of the Service's remote object instance
	//       - status and configuration parameters, as needed
	typeIfc         reflect.Type
	valIfc          reflect.Value
	valObj          reflect.Value
	isLossy         bool
	isDelayed       bool
	address         string
	isRunning       bool
	listener        net.Listener
	successfulCalls int
}

// build a new Service instance around a given struct of supported functions,
// a local instance of a corresponding object that supports these functions,
// and arguments to support creation and use of LeakySocket-wrapped connections.
// performs the following:
// -- returns a local error if function struct or object is nil
// -- returns a local error if any function in the struct is not a remote function
// -- if neither error, creates and populates a Service and returns a pointer
func NewService(ifc interface{}, sobj interface{}, port int, lossy bool, delayed bool) (*Service, error) {
	// Check for nil inputs
	if ifc == nil || sobj == nil {
		return nil, errors.New("NewService requires non-nil function struct and object")
	}

	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's Type
	if reflect.TypeOf(ifc).Elem().Kind() != reflect.Struct {
		return nil, errors.New("NewService requires a pointer to a function struct")
	}

	// if sobj is a pointer to an object instance, then
	// reflect.ValueOf(sobj) is the reflected object's Value
	if reflect.TypeOf(sobj).Kind() != reflect.Ptr {
		return nil, errors.New("NewService requires a pointer to an object instance")
	}

	// check if refleced struct include RemoteObjectError
	ifcType := reflect.TypeOf(ifc).Elem()

methods:
	for i := 0; i < ifcType.NumField(); i++ {
		field := ifcType.Field(i)
		for j := 0; j < field.Type.NumOut(); j++ {
			if field.Type.Out(j).String() == "remote.RemoteObjectError" {
				continue methods
			}
		}
		return nil, errors.New("not remote function")
	}

	// create a new Service instance and populate it with the given inputs
	fmt.Println("[INFO]\tcreate new service")
	gob.Register(RemoteObjectError{})
	service := &Service{
		typeIfc:   reflect.TypeOf(ifc).Elem(),
		valIfc:    reflect.ValueOf(ifc).Elem(),
		valObj:    reflect.ValueOf(sobj),
		isLossy:   lossy,
		isDelayed: delayed,
		address:   fmt.Sprintf("localhost:%d", port),
		isRunning: false,
		listener:  nil,
	}

	// get the Service ready to start
	return service, nil
}

// start the Service's tcp listening connection, update the Service
// status, and start receiving caller connections
func (serv *Service) Start() error {
	// attempt to start a Service created using NewService
	//
	// if called on a service that is already running, print a warning
	// but don't return an error or do anything else
	//
	// otherwise, start the multithreaded tcp server at the given address
	// and update Service state
	//
	// IMPORTANT: Start() should not be a blocking call. once the Service
	// is started, it should return
	//
	//
	// After the Service is started (not to be done inside of this Start
	//      function, but wherever you want):
	//
	// - accept new connections from client callers until someone calls
	//   Stop on this Service, spawning a thread to handle each one
	//
	// - within each client thread, wrap the net.Conn with a LeakySocket
	//   e.g., if Service accepts a client connection `c`, create a new
	//   LeakySocket ls as `ls := LeakySocket(c, ...)`.  then:
	//
	// 1. receive a byte-string on `ls` using `ls.RecvObject()`
	//
	// 2. decoding the byte-string
	//
	// 3. check to see if the service interface's Type includes a method
	//    with the given name
	//
	// 4. invoke method
	//
	// 5. encode the reply message into a byte-string
	//
	// 6. send the byte-string using `ls.SendObject`, noting that the configuration
	//    of the LossySocket does not guarantee that this will work...

	if serv.IsRunning() {
		fmt.Println("[WARNING]\tService is already running")
		return nil
	}

	// start the multithreaded tcp server and update Service state
	listener, err := net.Listen(RunningProtocol, serv.address)
	if err != nil {
		fmt.Println("[ERROR]\tunable to start the server: ", err)
		return errors.New("unable to start the server")
	}
	serv.listener = listener
	serv.isRunning = true
	go serv.serve()

	return nil

}

// serve listens to client requests at the given address.
// It continuously accepts incoming connections and handles them in separate goroutines.
func (serv *Service) serve() {
	// start the multithreaded tcp server at the given address
	fmt.Println("[INFO]\tserver running")
	for serv.isRunning {
		conn, err := serv.listener.Accept()

		if err != nil {
			fmt.Println("[ERROR]\tunable to accept the connection: ", err)
			break
		}
		go serv.handleConnection(conn)
	}
}

// handleConnection handles a single connection to the remote service.
// It receives a connection and performs the necessary operations to handle the request.
// It decodes the received byte-string, checks if the service interface's Type includes the method,
// validates the number and types of arguments, invokes the method, encodes the reply message,
// and sends the reply back to the client.
func (serv *Service) handleConnection(conn net.Conn) {
	// 1. receive a byte-string on `ls` using `ls.RecvObject()`
	// 2. decoding the byte-string
	// 3. check to see if the service interface's Type includes a method
	//    with the given name
	// 4. invoke method
	// 5. encode the reply message into a byte-string
	// 6. send the byte-string using `ls.SendObject`, noting that the configuration
	//    of the LossySocket does not guarantee that this will work...
	ls := NewLeakySocket(conn, serv.isLossy, serv.isDelayed)
	byteArray, err := ls.RecvObject()
	if err != nil {
		msg := fmt.Sprint("unable to receive the request message: ", err)
		reply := ReplyMsg{Success: false, Err: RemoteObjectError{msg}}
		SendReply(reply, ls)
		return
	}
	// decode the received byte-string
	buf := *bytes.NewBuffer(byteArray)
	dec := gob.NewDecoder(&buf)
	reply := RequestMsg{}
	err = dec.Decode(&reply)
	if err != nil {
		msg := fmt.Sprintln("unable to decode the reply message: ", err)
		reply := ReplyMsg{Success: false, Err: RemoteObjectError{msg}}
		SendReply(reply, ls)
		return
	}

	// check to see if the service interface's Type includes the method
	// method := serv.valObj.FieldByName(reply.Method)
	method := serv.valObj.MethodByName(reply.Method)
	if !method.IsValid() {
		msg := fmt.Sprintln("method not found")
		reply := ReplyMsg{Success: false, Err: RemoteObjectError{msg}}
		SendReply(reply, ls)
		return
	}

	if len(reply.Args) != method.Type().NumIn() {
		msg := "number of arguments is not correct"
		reply := ReplyMsg{Success: false, Err: RemoteObjectError{msg}}
		SendReply(reply, ls)
		return
	}

	// invoke method
	args := make([]reflect.Value, len(reply.Args))
	for i, arg := range reply.Args {
		if reflect.TypeOf(arg).String() != method.Type().In(i).String() {
			msg := "argument type is not correct"
			reply := ReplyMsg{Success: false, Err: RemoteObjectError{msg}}
			SendReply(reply, ls)
			return
		}
		args[i] = reflect.ValueOf(arg)
	}

	ret := method.Call(args)
	// encode the reply message into a byte-string

	repMsg := ReplyMsg{Success: true, Reply: make([]interface{}, 0)}
	for _, val := range ret {
		// TODO: check if there's error
		repMsg.Reply = append(repMsg.Reply, val.Interface())
		if val.Type().String() == "remote.RemoteObjectError" {
			repMsg.Err = val.Interface().(RemoteObjectError)
		}
	}
	if SendReply(repMsg, ls) {
		serv.successfulCalls++
	}
}

// SendReply sends a reply message to the server using the given LeakySocket.
// It encodes the reply message using gob encoding and sends it over the socket.
// If there is an error during encoding or sending, it prints an error message and closes the socket.
// It continues to retry sending the message until it is successfully sent or an error occurs.
// Finally, it closes the socket.
func SendReply(reply ReplyMsg, ls *LeakySocket) bool {
	fmt.Println("[INFO]\tsending reply:", reply)
	defer ls.Close()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(reply)
	if err != nil {
		msg := fmt.Sprintf("[ERROR]\tunable to encode the request message: %s", err)
		fmt.Println(msg)
		return false
	}

	for success := false; !success; {
		success, err = ls.SendObject(buf.Bytes())
		if err != nil {
			fmt.Println("[ERROR]\tunable to send to the server: ", err)
			return false
		}
	}
	return true
}

// GetCount returns the total number of remote calls served successfully by this Service.
func (serv *Service) GetCount() int {
	// TODO: return the total number of remote calls served successfully by this Service
	return 0
}

// IsRunning returns a boolean value indicating whether the service is currently running.
func (serv *Service) IsRunning() bool {
	return serv.isRunning
}

// Stop stops the Service, changes its state accordingly, and cleans up any resources.
// If the Service is not running, it returns immediately.
func (serv *Service) Stop() {
	if !serv.isRunning {
		return
	}
	fmt.Println("[INFO]\tstopping server")
	err := serv.listener.Close()
	if err != nil {
		fmt.Println("[ERROR]\tunable to close the server: ", err)
	}
	serv.isRunning = false
}

// StubFactory -- make a client-side stub
//
// StubFactory uses reflection to populate the interface functions to create the
// caller's stub interface. Only works if all functions are exported/public.
// Once created, the interface masks remote calls to a Service that hosts the
// object instance that the functions are invoked on.  The network address of the
// remote Service must be provided with the stub is created, and it may not change later.
// A call to StubFactory requires the following inputs:
// -- a struct of function declarations to act as the stub's interface/proxy
// -- the remote address of the Service as "<ip-address>:<port-number>"
// -- indicator of whether caller-to-callee channel has emulated packet loss
// -- indicator of whether caller-to-callee channel has emulated propagation delay
// performs the following:
// -- returns a local error if function struct is nil
// -- returns a local error if any function in the struct is not a remote function
// -- otherwise, uses relection to access the functions in the given struct and
//
//	populate their function definitions with the required stub functionality
func StubFactory(ifc interface{}, adr string, lossy bool, delayed bool) error {
	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's reflect.Type
	// and reflect.ValueOf(ifc).Elem() is the reflected struct's reflect.Value
	//
	// Here's what it needs to do (not strictly in this order):
	//
	//    1. create a request message populated with the method name and input
	//       arguments to send to the Service
	//
	//    2. create a []reflect.Value of correct size to hold the result to be
	//       returned back to the program
	//
	//    3. connect to the Service's tcp server, and wrap the connection in an
	//       appropriate LeakySocket using the parameters given to the StubFactory
	//
	//    4. encode the request message into a byte-string to send over the connection
	//
	//    5. send the encoded message, noting that the LeakySocket is not guaranteed
	//       to succeed depending on the given parameters
	//
	//    6. wait for a reply to be received using RecvObject, which is blocking
	//        -- if RecvObject returns an error, populate and return error output
	//
	//    7. decode the received byte-string according to the expected return types

	if ifc == nil {
		return errors.New("StubFactory requires non-nil function struct")
	}

	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's Type
	if reflect.TypeOf(ifc).Elem().Kind() != reflect.Struct {
		return errors.New("NewService requires a pointer to a function struct")
	}

	// check if refleced struct include RemoteObjectError
	ifcType := reflect.TypeOf(ifc).Elem()
	ifcVal := reflect.ValueOf(ifc).Elem()

methods:
	for i := 0; i < ifcType.NumField(); i++ {
		field := ifcType.Field(i)
		for j := 0; j < field.Type.NumOut(); j++ {
			if field.Type.Out(j).String() == "remote.RemoteObjectError" {
				giveImplementation(field, ifcVal.Field(i), adr)
				continue methods
			}
		}
		return errors.New("not remote function")
	}
	gob.Register(RemoteObjectError{})
	return nil
}

// giveImplementation is a function that sets the implementation of a function value using reflection.
// It connects to a server at the given address and sends a request message containing the function name and arguments.
// It then receives a reply message from the server and sets the return values of the function based on the reply.
// If any error occurs during the process, it returns an error value.
func giveImplementation(fnt reflect.StructField, fnv reflect.Value, adr string) {
	// Function implementation
	retrieveFromServer := func(in []reflect.Value) []reflect.Value {
		// connect to the server
		conn, err := net.Dial(RunningProtocol, adr)
		if err != nil {
			return reflectErr("unable to connect to the server", fnv)
		}
		ls := NewLeakySocket(conn, false, false)
		reqMsg := RequestMsg{Method: fnt.Name, Args: make([]interface{}, 0)}
		for _, val := range in {
			reqMsg.Args = append(reqMsg.Args, val.Interface())
		}

		// 4. encode the request message into a byte-string to send over the connection
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(reqMsg)
		if err != nil {
			msg := fmt.Sprintf("unable to encode the request message: %s", err)
			return reflectErr(msg, fnv)
		}

		_, err = ls.SendObject(buf.Bytes())
		if err != nil {
			msg := fmt.Sprintf("unable to send to the server: %s", err)
			return reflectErr(msg, fnv)
		}

		byteArray, err := ls.RecvObject()
		if err != nil {
			msg := fmt.Sprintf("unable to receive from the server: %s", err)
			return reflectErr(msg, fnv)
		}
		// convert byteArray to replyMsg
		buf = *bytes.NewBuffer(byteArray)
		dec := gob.NewDecoder(&buf)
		reply := ReplyMsg{}
		err = dec.Decode(&reply)
		if err != nil {
			msg := fmt.Sprintf("unable to decode the reply message: %s", err)
			return reflectErr(msg, fnv)
		}
		if !reply.Success {
			return reflectErr(reply.Err.Err, fnv)
		}
		if len(reply.Reply) != fnv.Type().NumOut() {
			msg := "number of return values is not correct"
			return reflectErr(msg, fnv)
		}

		ret := []reflect.Value{}
		for _, val := range reply.Reply {
			ret = append(ret, reflect.ValueOf(val))
		}
		return ret

	}
	v := reflect.MakeFunc(fnv.Type(), retrieveFromServer)
	fnv.Set(v)
}

// reflectErr is a utility function that creates and returns a slice of reflect.Value
// based on the output types of a given reflect.Value function.
// It checks if the output type is "remote.RemoteObjectError" and if so, creates a new
// RemoteObjectError with the provided error message. Otherwise, it sets the output value
// to the zero value of its type.
func reflectErr(msg string, fn reflect.Value) []reflect.Value {
	ret := make([]reflect.Value, fn.Type().NumOut())
	for i := 0; i < fn.Type().NumOut(); i++ {
		if fn.Type().Out(i).String() == "remote.RemoteObjectError" {
			roe := RemoteObjectError{Err: msg}
			ret[i] = reflect.ValueOf(roe)
		} else {
			ret[i] = reflect.Zero(fn.Type().Out(i))
		}
	}
	return ret
}
