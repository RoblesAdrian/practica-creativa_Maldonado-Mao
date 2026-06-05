var socket = io("/predictions");

socket.on("connect", function() {
  console.log("connected", socket.id);
});

document.getElementById("flight_delay_classification").addEventListener("submit", function(event) {
  event.preventDefault();
  document.getElementById("result").textContent = "Processing...";

  var formData = {
    DepDelay: document.querySelector("input[name='DepDelay']").value,
    Carrier: document.querySelector("input[name='Carrier']").value,
    FlightDate: document.querySelector("input[name='FlightDate']").value,
    Dest: document.querySelector("input[name='Dest']").value,
    FlightNum: document.querySelector("input[name='FlightNum']") ? document.querySelector("input[name='FlightNum']").value : "",
    Origin: document.querySelector("input[name='Origin']").value
  };

  socket.emit("predict_flight_delay", formData, function(response) {
    console.log("ack", response);
    if (response.status === "OK" && response.prediction) {
      document.getElementById("result").textContent = renderPrediction(response.prediction);
    } else {
      document.getElementById("result").textContent = "Waiting...";
    }
  });
});

function renderPrediction(response) {
  if (response.Prediction == 0 || response.Prediction == "0") return "Early (15+ Minutes Early)";
  if (response.Prediction == 1 || response.Prediction == "1") return "Slightly Early (0-15 Minute Early)";
  if (response.Prediction == 2 || response.Prediction == "2") return "Slightly Late (0-30 Minute Delay)";
  if (response.Prediction == 3 || response.Prediction == "3") return "Very Late (30+ Minutes Late)";
  return "Unknown prediction";
}