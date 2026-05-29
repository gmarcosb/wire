import Foundation

// Note: To use this in an actual project, the consumer must import GRPC, NIO, and NIOHPACK
// from grpc-swift-nio-transport, and the Kotlin module (e.g. WireGrpcClient).
// Since wire-runtime-swift does not export grpc-swift as a hard dependency to avoid bloating
// consumers that do not need gRPC, this class serves as an example of how one would
// bridge the NativeGrpcTransport from wire-grpc-client.

#if canImport(GRPC) && canImport(NIO) && canImport(NIOHPACK) && canImport(WireGrpcClient)

import GRPC
import NIO
import NIOHPACK
import WireGrpcClient

/// A reference implementation of the Kotlin NativeGrpcTransport interface
/// that bridges Wire's gRPC client with SwiftNIO's gRPC transport.
public class SwiftNioGrpcTransport: NativeGrpcTransport {
    private let channel: GRPCChannel
    private let group: EventLoopGroup

    public init(host: String, port: Int) {
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.channel = ClientConnection.insecure(group: group)
            .connect(host: host, port: port)
    }

    /// Called by Wire's NativeGrpcCall to execute a unary request.
    public func execute(
        url: String,
        requestMetadata: [String : String],
        requestMessage: WireGrpcClient.KotlinByteArray,
        timeout: WireGrpcClient.Timeout,
        completionHandler: @escaping (NativeGrpcResponse?, Error?) -> Void
    ) {
        var customHeaders = HPACKHeaders()
        for (key, value) in requestMetadata {
            customHeaders.add(name: key, value: value)
        }

        let callOptions = CallOptions(customMetadata: customHeaders)

        guard let urlObj = URL(string: url) else {
            completionHandler(nil, NSError(domain: "SwiftNioGrpcTransport", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid URL"]))
            return
        }

        let path = urlObj.path
        let requestData = Data(requestMessage.toByteArray())

        let call = channel.makeUnaryCall(
            path: path,
            request: requestData,
            callOptions: callOptions
        )

        call.response.whenComplete { result in
            switch result {
            case .success(let responseData):
                call.trailingMetadata.whenComplete { trailersResult in
                    let trailers = (try? trailersResult.get()) ?? HPACKHeaders()
                    let grpcStatus = trailers.first(name: "grpc-status").flatMap { Int($0) } ?? 0
                    let grpcMessage = trailers.first(name: "grpc-message")

                    var responseMetadata = [String: String]()
                    for trailer in trailers {
                        responseMetadata[trailer.name] = trailer.value
                    }

                    let response = NativeGrpcResponse(
                        status: 200,
                        responseMetadata: responseMetadata,
                        responseMessage: WireGrpcClient.KotlinByteArray(data: responseData),
                        grpcStatus: Int32(grpcStatus),
                        grpcMessage: grpcMessage,
                        grpcStatusDetails: nil
                    )

                    completionHandler(response, nil)
                }
            case .failure(let error):
                completionHandler(nil, error)
            }
        }
    }

    deinit {
        try? channel.close().wait()
        try? group.syncShutdownGracefully()
    }
}

extension WireGrpcClient.KotlinByteArray {
    convenience init(data: Data) {
        let intArray = data.map { Int8(bitPattern: $0) }
        self.init(size: Int32(data.count))
        for (index, byte) in intArray.enumerated() {
            self.set(index: Int32(index), value: byte)
        }
    }

    func toByteArray() -> [UInt8] {
        var array = [UInt8]()
        for i in 0..<self.size {
            let byte = UInt8(bitPattern: self.get(index: i))
            array.append(byte)
        }
        return array
    }
}

#endif
