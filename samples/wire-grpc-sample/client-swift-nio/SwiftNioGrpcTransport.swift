import Foundation
import GRPC
import NIO
import NIOHPACK
import WireGrpcClient // Assumes the exported Kotlin/Native module is named WireGrpcClient

/// A sample implementation of the Kotlin NativeGrpcTransport interface
/// that bridges Wire's gRPC client with SwiftNIO's gRPC transport.
class SwiftNioGrpcTransport: NativeGrpcTransport {
    private let channel: GRPCChannel
    private let group: EventLoopGroup

    init(host: String, port: Int) {
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.channel = ClientConnection.insecure(group: group)
            .connect(host: host, port: port)
    }

    /// Called by Wire's NativeGrpcCall to execute a unary request.
    func execute(
        url: String,
        requestMetadata: [String : String],
        requestMessage: WireGrpcClient.KotlinByteArray, // Using KotlinByteArray from Kotlin Interop
        timeout: WireGrpcClient.Timeout,
        completionHandler: @escaping (NativeGrpcResponse?, Error?) -> Void
    ) {
        // Prepare options with headers
        var customHeaders = HPACKHeaders()
        for (key, value) in requestMetadata {
            customHeaders.add(name: key, value: value)
        }

        let callOptions = CallOptions(customMetadata: customHeaders)

        // Ensure path matches gRPC expected format (e.g., "/routeguide.RouteGuide/GetFeature")
        guard let urlObj = URL(string: url) else {
            completionHandler(nil, NSError(domain: "SwiftNioGrpcTransport", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid URL"]))
            return
        }

        let path = urlObj.path
        let requestData = Data(requestMessage.toByteArray())

        // Execute the call using the generic unary method on the channel
        let call = channel.makeUnaryCall(
            path: path,
            request: requestData,
            callOptions: callOptions
        )

        // Wait for response and handle bridging back to Kotlin
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

                    // Construct the NativeGrpcResponse expected by Wire Kotlin Native
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
                // Error on the SwiftNIO side
                completionHandler(nil, error)
            }
        }
    }

    deinit {
        try? channel.close().wait()
        try? group.syncShutdownGracefully()
    }
}

// MARK: - Helpers for Data/ByteArray conversion
extension WireGrpcClient.KotlinByteArray {
    /// Helper to bridge Data to KotlinByteArray
    convenience init(data: Data) {
        let intArray = data.map { Int8(bitPattern: $0) }
        self.init(size: Int32(data.count))
        for (index, byte) in intArray.enumerated() {
            self.set(index: Int32(index), value: byte)
        }
    }

    /// Helper to bridge KotlinByteArray to Data
    func toByteArray() -> [UInt8] {
        var array = [UInt8]()
        for i in 0..<self.size {
            let byte = UInt8(bitPattern: self.get(index: i))
            array.append(byte)
        }
        return array
    }
}
